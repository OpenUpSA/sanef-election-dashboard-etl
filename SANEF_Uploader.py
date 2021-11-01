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

async def get_api_data(url, query, session):
    api_url = IEC_API + url + str(query)
    try:
        response = await session.request(method='GET', url=api_url, headers=headers)
        response.raise_for_status()
    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error ocurred: {err}")
    response_json = await response.json()
    return response_json

def upload():
    print('Saving to CSV...')

    time = datetime.now().strftime("%d%m%Y-%H%M")
    file = IEC_ENDPOINT + '.' + time + '.csv'

    Results_df = pd.DataFrame(Results)
    Results_df.to_csv(file, index=False)
    
    print('CSV Done')

    print('Uploading')
    
    url = f"{WAZI_ENDPOINT}/api/v1/datasets/{DATASET_ID}/upload/"

    headers = {'authorization': f"Token {WAZI_TOKEN}"}
    files = {'file': open(file, 'rb')}
    payload = {'update': False, 'overwrite': True}

    wazi_r = requests.post(url, headers=headers, data=payload, files=files)
    wazi_r.raise_for_status()

    print('Done')

async def run_program(url, query, session):
    try:
        
        
        ##### 
        ## WARD: VOTES BY PARTY (1378)
        #####
        
        if(IEC_ENDPOINT == 'ward_votes_by_party'):

            response = await get_api_data(url, query, session)

            if(response['bResultsComplete'] == True):
                for result in response['PartyBallotResults']:
                    Results.append(
                        {
                            'Geography': response['WardID'],
                            'Party': result['Name'],
                            'Count': result['TotalValidVotes'],
                        }
                    )

        ##### 
        ## VOTER TURNOUT (1386)
        #####
        
        if(IEC_ENDPOINT == 'voter_turnout'):

            response = await get_api_data(url, query, session)

            if(response['bResultsComplete'] == True):
                Results.append(
                    {
                        'Geography': response['WardID'],
                        'Voter Turnout': "Didn't vote",
                        'Count': response['RegisteredVoters'] - response['TotalVotesCast'],
                    }
                )
                Results.append(
                    {
                        'Geography': response['WardID'],
                        'Voter Turnout': 'Voted',
                        'Count': response['TotalVotesCast'],
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

                sqlquery = "SELECT * FROM LED_GIS_Display_Ward_WardCandidates WHERE fklEEId = " + ELECTORAL_EVENT_ID
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

            if(RESET_DATASET == 'reset'):
                Results.append(
                    {
                        'Geography': 'None',
                        'Contents': '-'
                    }
                )

            else:
               
                sqlquery =  "SELECT fklMunicipalityID, pkfklWardID, pkfklCandidateID, PCR_Candidates.sIDNo, PCR_Candidates.sSurname, PCR_Candidates.sInitials, PCR_Candidates.sFullName, PCR_Party.sPartyName FROM LED_GIS_WardWinners LEFT JOIN PCR_Candidates ON LED_GIS_WardWinners.pkfklCandidateID=PCR_Candidates.pklCandidateID LEFT JOIN PCR_Party ON LED_GIS_WardWinners.pkfklPartyID=PCR_Party.pklPartyID WHERE pkfklEEID = " + ELECTORAL_EVENT_ID
                cursor = conn.cursor()
                cursor.execute(sqlquery)

                for row in cursor:

                    Results.append(
                        {
                            'Geography': row[1],
                            'Contents': row[6] + ' ' + row[4] + ' - ' + row[7],
                        }
                    )
            

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

                sqlquery =  "SELECT * FROM LED_GIS_Display_Ward WHERE fklEEId = " + ELECTORAL_EVENT_ID
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
                upload()

            else:

                parties = []
                partiesquery =  "SELECT * FROM PCR_Party"
                cursor = conn.cursor()
                cursor.execute(partiesquery)

                for row in cursor:
                    parties.append(row)


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
                    print(row['bHung_x'])


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
        ## COUNCILS WON BY PARTY (1385)
        #####

        if(IEC_ENDPOINT == 'councils_won_by_party'):


            if(RESET_DATASET == 'reset'):

                return

            else:

                # {
                #     'Geography':
                #     'Party Name':
                #     'Count':
                # }

                return
                
            


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
                upload()
                
            else:

                sqlquery =  "SELECT * FROM LED_GIS_Display_Municipal WHERE fklEEId = " + ELECTORAL_EVENT_ID
                cursor = conn.cursor()
                cursor.execute(sqlquery)

                for row in cursor:

                    muni = munis_df.loc[munis_df.MunicipalityID == row[2]]['Municipality'].values[0]

                    Results.append(
                        {
                            'Geography': muni,
                            'Party Name': row[4],
                            'Seat Type': 'Ward',
                            'Count': row[9]
                        }
                    )
                    Results.append(
                        {
                            'Geography': muni,
                            'Party Name': row[4],
                            'Seat Type': 'PR',
                            'Count': row[13]
                        }
                    )

            
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
                await asyncio.gather(*[run_program('/api/v1/LGEBallotResults?ElectoralEventID=' + str(ELECTORAL_EVENT_ID), '&ProvinceID=' + str(ward[0]) + '&MunicipalityID=' + str(ward[1]) + '&WardID=' + str(ward[2]), session) for ward in Wards])
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
                await asyncio.gather(*[run_program('/api/v1/LGEBallotResults?ElectoralEventID=' + str(ELECTORAL_EVENT_ID), '&ProvinceID=' + str(ward[0]) + '&MunicipalityID=' + str(ward[1]) + '&WardID=' + str(ward[2]), session) for ward in Wards])
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
            await run_program('','',session)
            upload()

        ##### 
        ## PR VOTES BY PARTY (1380)
        #####

        if(IEC_ENDPOINT == 'pr_votes_by_party'):
            await run_program('','',session)
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
            # upload()
        
    

        ##### 
        ## SEATS WON (1383)
        #####

        if(IEC_ENDPOINT == 'seats_won'):
            await run_program('','',session)
            upload()

        
        

asyncio.run(main())