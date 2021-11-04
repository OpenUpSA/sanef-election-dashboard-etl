import pandas as pd
import requests
import asyncio
import sys
import pyodbc
from datetime import datetime
from aiohttp import ClientSession
from urllib.error import HTTPError

WAZI_ENDPOINT = sys.argv[1]
WAZI_TOKEN = sys.argv[2]
DATASET_ID = sys.argv[3]

IEC_TOKEN = sys.argv[4]
IEC_ENDPOINT = sys.argv[5]

DB_SERVER = sys.argv[6]
DB = sys.argv[7]
DB_USERNAME = sys.argv[8]
DB_PASSWORD = sys.argv[9]

RESET_DATASET = sys.argv[10]

IEC_API = "https://api.elections.org.za"
ELECTORAL_EVENT_ID = '1091'

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + DB_SERVER + ';DATABASE=' + DB + ';UID=' + DB_USERNAME + ';PWD=' + DB_PASSWORD)

headers = {"Authorization": "Bearer " + IEC_TOKEN}

Results = []

wards_df = pd.read_csv('./delimitations/Wards.csv')
Wards = wards_df.values.tolist()
munis_df = pd.read_csv('./delimitations/Munis.csv')
Munis = munis_df.values.tolist()
munis_map = pd.read_csv('./delimitations/MunisMap.csv')

async def get_api_data(url, query, session):
    api_url = IEC_API + url + str(query)
    try:
        response = await session.request(method='GET', url=api_url, headers=headers)
        response.raise_for_status()
    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error ocurred: {err}")
    response_json = await response.json(content_type=None)
    return response_json

def upload():
    time = datetime.now().strftime("%d%m%Y-%H%M")
    file = IEC_ENDPOINT + '.' + time + '.csv'

    Results_df = pd.DataFrame(Results)
    Results_df.to_csv('datasets/' + file, index=False)

    url = f"{WAZI_ENDPOINT}/api/v1/datasets/{DATASET_ID}/upload/"

    headers = {'authorization': f"Token {WAZI_TOKEN}"}
    files = {'file': open('datasets/' + file, 'rb')}
    payload = {'update': True, 'overwrite': True}

    wazi_r = requests.post(url, headers=headers, data=payload, files=files)
    wazi_r.raise_for_status()

async def run_program(url, query, session):
    try:


        #####
        ## WARD: VOTES BY PARTY (1378)
        #####

        if(IEC_ENDPOINT == 'ward_votes_by_party'):

            response = await get_api_data(url, query, session)

            for result in response['PartyBallotResults']:
                Results.append(
                    {
                        'Geography': response['WardID'],
                        'Party': result['Name'],
                        'Count': result['TotalValidVotes'],
                    }
                )

       
        

        #####
        ## WARD: VOTES BY CANDIDATE (1379)
        #####

        if(IEC_ENDPOINT == 'ward_votes_by_candidate'):

            if(RESET_DATASET == 'reset'):
                Results.append(
                    {
                        'Geography': 'None',
                        'Party': '-',
                        'Count': 0
                    }
                )

            else:

                completed_wards = await check_completed_wards()


                for ward in completed_wards:
                    
                    sqlquery = "SELECT * FROM LED_GIS_Display_Ward_WardCandidates WHERE fklWardId = " + str(ward[2]) + " AND fklEEId = " + str(ELECTORAL_EVENT_ID)
                    cursor = conn.cursor()
                    cursor.execute(sqlquery)

                    for row in cursor:

                        Results.append(
                            {
                                'Geography': row[3],
                                'Party': row[9] + ' - ' + row[5],
                                'Count': row[10]
                            }
                        )    
                



        #####
        ## WARD: COUNCILLOR ELECTED (1382)
        #####

        if(IEC_ENDPOINT == 'ward_councillor_elected'):
            
            response = await get_api_data(url, query, session)

            for candidate in response:
                
                candidate_entry = {
                    'Geography': candidate['WardID'],
                    'Contents': candidate['Name'] + ' - ' + candidate['PartyName'],
                }

                list_of_all_values = [value for elem in Results for value in elem.values()]
                value = candidate['WardID']

                if value not in list_of_all_values:
                    Results.append(candidate_entry)


        #####
        ## PR VOTES BY PARTY (1380)
        #####

        if(IEC_ENDPOINT == 'pr_votes_by_party'):

            if(RESET_DATASET == 'reset'):
                Results.append(
                    {
                        'Geography': 'None',
                        'Party': '-',
                        'Count': 0
                    }
                )

            else:

                completed_wards = await check_completed_wards()

                for ward in completed_wards:

                    sqlquery =  "SELECT * FROM LED_GIS_Display_Ward WHERE fklWardId = " + str(ward[2]) + " AND fklEEId = " + ELECTORAL_EVENT_ID
                    cursor = conn.cursor()
                    cursor.execute(sqlquery)

                    for row in cursor:

                        Results.append(
                            {
                                'Geography': row[3],
                                'Party': row[5],
                                'Count': row[10]
                            }
                        )

                    

        #####
        ## HUNG OUTRIGHT MAJORITY COUNCILS (1384)
        #####

        if(IEC_ENDPOINT == 'hung_councils'):


            if(RESET_DATASET == 'reset'):

                Results.append(
                    {
                        'Geography': 'None',
                        'Councils': '-',
                        'Count': 0
                    }
                )


            else:

                sqlquery =  "SELECT * FROM LED_GIS_CouncilWinners WHERE fklEEId = " + ELECTORAL_EVENT_ID
                cursor = conn.cursor()
                cursor.execute(sqlquery)

                council_winners = []

                for row in cursor:
                    row_to_list = [elem for elem in row]
                    council_winners.append(row_to_list)


                columns = ['pklCouncilWinnerID', 'fklEEID', 'fklMunicipalityID', 'fklPartyID', 'fklLeadingPartyID', 'fklMajorityPartyID', 'lCouncilSeatsAvailable', 'lTotalPartySeatsWon', 'bDraw', 'bHung']

                df = pd.DataFrame(council_winners, columns=columns)

                dff = pd.merge(munis_df, df, left_on='MunicipalityID', right_on='fklMunicipalityID', how='inner')

                dff['ProvinceID'] = dff['ProvinceID'].astype(str)
                dff['ProvinceID'] = dff['ProvinceID'].map({'9': 'WC', '8': 'NW', '7':'LIM', '6':'NC','5':'MP', '4':'KZN', '3':'GT','2':'FS', '1':'EC'})

                hungCouncils = dff.groupby(['ProvinceID'])['bHung'].sum()
                totalCouncils = dff.groupby(['ProvinceID'])['bHung'].count()

                dff2 = pd.merge(hungCouncils, totalCouncils, left_on='ProvinceID', right_on='ProvinceID', how='inner')

                for index, row in dff2.iterrows():

                    Results.append(
                        {
                            'Geography': index,
                            'Councils': 'Hung',
                            'Count': row['bHung_x']
                        }
                    )
                    Results.append(
                        {
                            'Geography': index,
                            'Councils': 'Outright Majority',
                            'Count': row['bHung_y'] - row['bHung_x']
                        }
                    )


        #####
        ## LIST OF HUNG COUNCILS (1424)
        #####

        if(IEC_ENDPOINT == 'list_of_hung_councils'):


            if(RESET_DATASET == 'reset'):

                Results.append(
                    {
                        'Geography': 'None',
                        'Contents': '-'
                    }
                )


            else:

                sqlquery =  "SELECT * FROM LED_GIS_CouncilWinners WHERE bHung = 1 AND fklEEId = " + ELECTORAL_EVENT_ID
                cursor = conn.cursor()
                cursor.execute(sqlquery)

                columns = ['pklCouncilWinnerID', 'fklEEID', 'fklMunicipalityID', 'fklPartyID', 'fklLeadingPartyID', 'fklMajorityPartyID', 'lCouncilSeatsAvailable', 'lTotalPartySeatsWon', 'bDraw', 'bHung']

                df = pd.DataFrame([tuple(t) for t in cursor], columns=columns)

                dff = pd.merge(munis_df, df, left_on='MunicipalityID', right_on='fklMunicipalityID', how='inner')

                dff['ProvinceID'] = dff['ProvinceID'].astype(str)
                dff['ProvinceID'] = dff['ProvinceID'].map({'9': 'WC', '8': 'NW', '7':'LIM', '6':'NC','5':'MP', '4':'KZN', '3':'GT','2':'FS', '1':'EC'})

                hung_councils = dff.groupby(['ProvinceID'])


                for geo, group in hung_councils:

                    contents = '<ul>'
                    for index, row in group.iterrows():
                        contents += '<li><a href = https://sanef-local-gov.openup.org.za/#geo:' + row['Municipality'] + '>' + row['Municipality'] + ' - ' + row['MunicipalityName'] + ' </a> </li>'

                    contents += '</ul>'

                    Results.append(
                        {
                            'Geography': geo,
                            'Contents': contents
                        }
                    )


        #####
        ## COUNCILS WON BY PARTY (1385)
        #####

        if(IEC_ENDPOINT == 'councils_won_by_party'):


            if(RESET_DATASET == 'reset'):

                Results.append(
                    {
                        'Geography': 'None',
                        'Party Name': '-',
                        'Count': 0
                    }
                )

            else:

                partiesquery =  "SELECT * FROM PCR_Party"
                cursor = conn.cursor()
                cursor.execute(partiesquery)

                columns = ['pklPartyID','sPartyName','sPartyAbbr']

                parties_df = pd.DataFrame([tuple(t) for t in cursor], columns=columns)

                sqlquery =  "SELECT * FROM LED_GIS_CouncilWinners WHERE bHung = 0 AND fklEEId = " + ELECTORAL_EVENT_ID
                cursor = conn.cursor()
                cursor.execute(sqlquery)

                council_winners = []

                for row in cursor:
                    row_to_list = [elem for elem in row]
                    council_winners.append(row_to_list)


                columns = ['pklCouncilWinnerID', 'fklEEID', 'fklMunicipalityID', 'fklPartyID', 'fklLeadingPartyID', 'fklMajorityPartyID', 'lCouncilSeatsAvailable', 'lTotalPartySeatsWon', 'bDraw', 'bHung']

                df = pd.DataFrame(council_winners, columns=columns)

                dff = pd.merge(munis_df, df, left_on='MunicipalityID', right_on='fklMunicipalityID', how='inner')
                dff = pd.merge(parties_df,dff, left_on='pklPartyID', right_on='fklPartyID', how='inner')

                dff['ProvinceID'] = dff['ProvinceID'].astype(str)
                dff['ProvinceID'] = dff['ProvinceID'].map({'9': 'WC', '8': 'NW', '7':'LIM', '6':'NC','5':'MP', '4':'KZN', '3':'GT','2':'FS', '1':'EC'})

                councils_by_party = dff.groupby(['ProvinceID','sPartyName'])

                for geo, group in councils_by_party:

                    Results.append(
                        {
                            'Geography': geo[0],
                            'Party': geo[1],
                            'Count': group.count()['sPartyName']
                        }
                    )



        #####
        ## SEATS WON (1383)
        #####

        if(IEC_ENDPOINT == 'seats_won'):

            response = await get_api_data(url, query, session)

            return response



            
    except Exception as err:
        print(f"Exception occured: {err}")
        pass



async def check_completed_wards():
    try:
        sqlquery = """
        select distinct EE_VotingDistricts.fklWardId
        from EE_VotingDistricts
        left join (
            select fklWardId , fklVotingDistrict 
            from (
                select fklWardId , fklVotingDistrict, sum(lTotalVotesCast) as VDTotalVotesCast 
                from LED_GIS_Display_VotingDistrict
                where fklEEId = 1091
                group by fklWardId, fklVotingDistrict 
            ) VDVotesCast
            where VDTotalVotesCast = 0
        ) UnfinishedDistricts on EE_VotingDistricts.fklWardID = UnfinishedDistricts.fklWardId
        where pkfklDelimID  = 78
        and UnfinishedDistricts.fklWardId is NULL
        order by EE_VotingDistricts.fklWardId
        """
        cursor = conn.cursor()
        cursor.execute(sqlquery)

        completed_wards = []


        for row in cursor:
            ward = wards_df.loc[wards_df['WardID'] == row[0]].values[0]

            completed_wards.append([ward[0],ward[1],ward[2]])

        return completed_wards
    
    except Exception as err:
        print(f"Exception occured: {err}")
        pass


async def main():
    async with ClientSession() as session:

        #####
        ## WARD: VOTES BY PARTY (1378)
        #####

        if(IEC_ENDPOINT == 'ward_votes_by_party'):

            if(RESET_DATASET == 'reset'):
                Results.append(
                    {
                        'Geography': 'None',
                        'Party': '-',
                        'Count': 0
                    }
                )
                upload()

            else:

                completed_wards = await check_completed_wards()

                await asyncio.gather(*[run_program('/api/v1/LGEBallotResults?ElectoralEventID=' + str(ELECTORAL_EVENT_ID), '&ProvinceID=' + str(ward[0]) + '&MunicipalityID=' + str(ward[1]) + '&WardID=' + str(ward[2]), session) for ward in completed_wards])
                upload()

        #####
        ## VOTER TURNOUT (1386)
        #####

        if(IEC_ENDPOINT == 'voter_turnout'):

            if(RESET_DATASET == 'reset'):
                Results.append(
                    {
                        'Geography': 'None',
                        'Voter Turnout': '-',
                        'Count': 0
                    }
                )
                upload()

            else:

                completed_wards = await check_completed_wards()

                for ward in completed_wards:
                    
                    sqlquery =  "Select fklWardID, lRegisteredVoters, sum(Fact_LGE_Master_VDStats.lVoterTurnout) as votes from Fact_LGE_Master_VDStats where fklWardID = " + str(ward[2]) + " and pkfklEEID = " + str(ELECTORAL_EVENT_ID) + " group by fklWardID, lRegisteredVoters order by fklWardID" 
                    df = pd.read_sql(sqlquery,conn)

                    df['tvoters'] = df['lRegisteredVoters'].sum()
                    df['tvotes'] = df['votes'].sum()

                    df = df.drop_duplicates(subset='fklWardID')

                    for index, row in df.iterrows():

                        ward_votes_voted = {
                            'Geography': row['fklWardID'],
                            'Voter Turnout': 'Voted',
                            'Count': row['tvotes']
                        }

                        ward_votes_didnt_vote = {
                            'Geography': row['fklWardID'],
                            'Voter Turnout': "Didn't Vote",
                            'Count': row['tvoters'] - row['tvotes']
                        }

                        Results.append(ward_votes_voted)
                        Results.append(ward_votes_didnt_vote)

                # AGGREGATE UP TO MUNI

                results_df = pd.DataFrame(Results)
                wards_ex = pd.merge(wards_df, munis_df, on=['ProvinceID','MunicipalityID'], how="inner").reset_index(drop=True)
                votes_ex = pd.merge(results_df, wards_ex, left_on=['Geography'], right_on=['WardID'], how="inner").reset_index(drop=True)

                voted = votes_ex.loc[votes_ex['Voter Turnout'] == "Voted"]
                voted_agg = voted.groupby(by=['MunicipalityID'])['Count'].sum()

                didnt_vote = votes_ex.loc[votes_ex['Voter Turnout'] == "Didn't Vote"]
                didnt_vote_agg = didnt_vote.groupby(by=['MunicipalityID'])['Count'].sum()

                muni_turnout = pd.merge(voted_agg,didnt_vote_agg, on="MunicipalityID").rename(columns={'Count_x':'Voted','Count_y':'Didnt'}).reset_index()

                muni_turnout_df = pd.merge(muni_turnout,munis_df, on="MunicipalityID")

                for index, row in muni_turnout_df.iterrows():

                    ward_votes_voted = {
                        'Geography': row['Municipality'],
                        'Voter Turnout': 'Voted',
                        'Count': row['Voted']
                    }

                    ward_votes_didnt_vote = {
                        'Geography': row['Municipality'],
                        'Voter Turnout': "Didn't Vote",
                        'Count': row['Didnt']
                    }

                    Results.append(ward_votes_voted)
                    Results.append(ward_votes_didnt_vote)

                # AGGREGATE UP TO DISTRICT/METRO

                muni_turnout_df2 = pd.merge(muni_turnout_df,munis_map, on="MunicipalityID")
                voted_agg2 = muni_turnout_df2.groupby('ParentID')['Voted'].sum()

                didnt_vote_agg2 = muni_turnout_df2.groupby('ParentID')['Didnt'].sum()

                district_turnout = pd.merge(voted_agg2,didnt_vote_agg2, on="ParentID")

                district_turnout_df = pd.merge(district_turnout,munis_df, left_on="ParentID", right_on="MunicipalityID")

                for index, row in district_turnout_df.iterrows():

                    district_votes_voted = {
                        'Geography': row['Municipality'],
                        'Voter Turnout': 'Voted',
                        'Count': row['Voted']
                    }

                    district_votes_didnt_vote = {
                        'Geography': row['Municipality'],
                        'Voter Turnout': "Didn't Vote",
                        'Count': row['Didnt']
                    }

                    Results.append(district_votes_voted)
                    Results.append(district_votes_didnt_vote)

                # AGGREGATE UP TO PROVINCE

                district_turnout_df['ProvinceID'] = district_turnout_df['ProvinceID'].astype(str)
                district_turnout_df['ProvinceID'] = district_turnout_df['ProvinceID'].map({'9': 'WC', '8': 'NW', '7':'LIM', '6':'NC','5':'MP', '4':'KZN', '3':'GT','2':'FS', '1':'EC'})

                voted_agg3 = district_turnout_df.groupby('ProvinceID')['Voted'].sum()

                didnt_vote_agg3 = district_turnout_df.groupby('ProvinceID')['Didnt'].sum()

                provincial_turnout = pd.merge(voted_agg3,didnt_vote_agg3, on="ProvinceID").reset_index()

       
                for index, row in provincial_turnout.iterrows():

                    provincial_votes_voted = {
                        'Geography': row['ProvinceID'],
                        'Voter Turnout': 'Voted',
                        'Count': row['Voted']
                    }

                    provincial_votes_didnt_vote = {
                        'Geography': row['ProvinceID'],
                        'Voter Turnout': "Didn't Vote",
                        'Count': row['Didnt']
                    }

                    Results.append(provincial_votes_voted)
                    Results.append(provincial_votes_didnt_vote)

                # NATIONAL

                ward_votes_voted = {
                    'Geography': 'ZA',
                    'Voter Turnout': 'Voted',
                    'Count': provincial_turnout['Voted'].sum()
                }

                ward_votes_didnt_vote = {
                    'Geography': 'ZA',
                    'Voter Turnout': "Didn't Vote",
                    'Count': provincial_turnout['Didnt'].sum()
                }

                Results.append(ward_votes_voted)
                Results.append(ward_votes_didnt_vote)


                upload()

        #####
        ## WARD: VOTES BY CANDIDATE (1379)
        #####

        if(IEC_ENDPOINT == 'ward_votes_by_candidate'):
            await run_program('','',session)
            upload()

        #####
        ## WARD: COUNCILLOR ELECTED (1382)
        #####

        if(IEC_ENDPOINT == 'ward_councillor_elected'):
            if(RESET_DATASET == 'reset'):
                Results.append(
                    {
                        'Geography': 'None',
                        'Contents': '-'
                    }
                )
                upload()

            else:
                await asyncio.gather(*[run_program('/api/v1/CouncilorsByEvent?ElectoralEventID=' + str(ELECTORAL_EVENT_ID), '&ProvinceID=' + str(province), session) for province in [1]])
                upload()

        #####
        ## PR VOTES BY PARTY (1380)
        #####

        if(IEC_ENDPOINT == 'pr_votes_by_party'):
            await run_program('','',session)
            upload()

        #####
        ## HUNG / OUTRIGHT MAJORITY COUNCILS (1384)
        #####

        if(IEC_ENDPOINT == 'hung_councils'):
            await run_program('','',session)
            upload()

        #####
        ## COUNCILS WON BY PARTY (1385)
        #####

        if(IEC_ENDPOINT == 'councils_won_by_party'):
            await run_program('','',session)
            upload()

        #####
        ## LIST OF HUNG COUNCILS (1424)
        #####

        if(IEC_ENDPOINT == 'list_of_hung_councils'):
            await run_program('','',session)
            upload()

        #####
        ## SEATS WON (1383)
        #####

        if(IEC_ENDPOINT == 'seats_won'):

            if(RESET_DATASET == 'reset'):
                Results.append(
                    {
                        'Geography': 'None',
                        'Party Name': '-',
                        'Seat Type': 'Ward',
                        'Count': 0
                    }
                )

            else:

                response = await asyncio.gather(*[run_program('/api/v1/LGESeatCalculationResults?ElectoralEventID=' + str(ELECTORAL_EVENT_ID), '&ProvinceID=' + str(muni[0]) + '&MunicipalityID=' + str(muni[1]), session) for muni in Munis])


                for muni in response:
                    if muni is not None:
                        for party in muni['PartyResults']:

                            munigeo = munis_df.loc[munis_df.MunicipalityID == muni['MunicipalityID']]['Municipality'].values[0]

                            Results.append(
                                {
                                    'Geography': munigeo,
                                    'Party Name': party['Name'],
                                    'Seat Type': 'Ward',
                                    'Count': party['WardSeats']
                                }
                            )
                            Results.append(
                                {
                                    'Geography': munigeo,
                                    'Party Name': party['Name'],
                                    'Seat Type': 'PR',
                                    'Count': party['PRSeats']
                                }
                            )         



                
                upload()


asyncio.run(main())
