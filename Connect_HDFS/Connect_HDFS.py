import pika

# Crawl data
# IMPORT LIBRARIES
import pandas as pd 
import numpy as np
from bs4 import BeautifulSoup, Tag
import requests

# CRAWL ALL PLAYER FROM FIFAWEB
player_list = pd.DataFrame()
url = 'https://www.fifaindex.com/players/{}/'
pageno = 1

credentials = pika.PlainCredentials('score', 'score')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='18.179.142.124', credentials=credentials))
channel = connection.channel()
channel.queue_declare (queue = 'hello' )

for i in range(300):
    webpage = url.format(pageno)
    print(webpage)
    response1 = requests.get(webpage)
    if response1.status_code != 200:
        break
    mainpage = response1.text
    mainsoup = BeautifulSoup(mainpage,'lxml')
    for tag in mainsoup.find_all('td',attrs={'data-title':'Name'}):
        
        webpage2 = 'https://www.fifaindex.com'+str(tag.find('a')['href'])
        # print(webpage2)
        response2 = requests.get(webpage2)
        subpage = response2.text
        soup = BeautifulSoup(subpage,'lxml')

        info = soup.find_all('h5', class_='card-header')

        if isinstance(info[0].contents[0], Tag):
            info_idx = 1
        else:
            info_idx = 0

        Player = soup.find_all('h5', class_='card-header')[info_idx].contents[0]
        Overall_Score = soup.find_all('h5', class_='card-header')[info_idx].contents[1].contents[0].text  
        Potential_Score = soup.find_all('h5', class_='card-header')[info_idx].contents[1].contents[-1].text
        Country = soup.find_all('a', class_="link-nation")[1].text
        if Country in ['Wales', 'England', 'Scotland', 'Northern Ireland']:
            Country = 'United Kingdom'
        Club = soup.find_all('a', class_="link-team")[1].text
        if len(soup.find_all('p', class_='data-currency data-currency-euro')) == 0:
          continue       
        Market_Value = ''.join([i for i in soup.find_all('p', class_='data-currency data-currency-euro')[0].findNext().text if i.isdigit()])
        Weekly_Salary = ''.join([i for i in soup.find_all('p', class_='data-currency data-currency-euro')[1].findNext().text if i.isdigit()])
        Height = soup.find_all('span', class_="data-units data-units-metric")[0].text.split()[0]   
        Weight = soup.find_all('span', class_="data-units data-units-metric")[1].text.split()[0]   
        Age = soup.find(text='Age ').findNext().text
        Preferred_Foot = soup.find(text='Preferred Foot ').findNext().text
        Position_Arr = ','.join([pos.text for pos in soup.find_all('a', class_="link-position")])
        Player_Work_Rate = soup.find(text='Player Work Rate ').findNext().text.replace(' ', '')

        Kit_Number = float(soup.find(text='Kit Number ').findNext().text)
        Joined_Club = soup.find(text='Joined Club ').findNext().text
        Contract_Length = float(soup.find(text='Contract Length ').findNext().text)

        Ball_Control = float(soup.find(text='Ball Control ').findNext().text)
        Dribbling = float(soup.find(text='Dribbling ').findNext().text)
        Ball_Skills = np.mean([Ball_Control,Dribbling])

        Marking = float(soup.find(text='Marking ').findNext().text)
        Slide_Tackle = float(soup.find(text='Slide Tackle ').findNext().text)
        Stand_Tackle = float(soup.find(text='Stand Tackle ').findNext().text)
        Defence = np.mean([Marking,Slide_Tackle,Stand_Tackle])

        Aggression = float(soup.find(text='Aggression ').findNext().text)
        Reactions = float(soup.find(text='Reactions ').findNext().text)
        Attack_Position = float(soup.find(text='Att. Position ').findNext().text)
        Interceptions = float(soup.find(text='Interceptions ').findNext().text)
        Vision = float(soup.find(text='Vision ').findNext().text)
        Composure = float(soup.find(text='Composure ').findNext().text)
        Mental = np.mean([Aggression,Reactions,Attack_Position,Interceptions,Vision,Composure])

        Crossing = float(soup.find(text='Crossing ').findNext().text)
        Short_Pass = float(soup.find(text='Short Pass ').findNext().text)
        Long_Pass = float(soup.find(text='Long Pass ').findNext().text)
        Passing = np.mean([Crossing,Short_Pass,Long_Pass])

        Acceleration = float(soup.find(text='Acceleration ').findNext().text)
        Stamina = float(soup.find(text='Stamina ').findNext().text)
        Strength = float(soup.find(text='Strength ').findNext().text)
        Balance = float(soup.find(text='Balance ').findNext().text)
        Sprint_Speed = float(soup.find(text='Sprint Speed ').findNext().text)
        Agility = float(soup.find(text='Agility ').findNext().text)
        Jumping = float(soup.find(text='Jumping ').findNext().text)
        Physical = np.mean([Acceleration,Stamina,Strength,Balance,Sprint_Speed,Agility,Jumping])

        Heading = float(soup.find(text='Heading ').findNext().text)
        Shot_Power = float(soup.find(text='Shot Power ').findNext().text)
        Finishing = float(soup.find(text='Finishing ').findNext().text)
        Long_Shots = float(soup.find(text='Long Shots ').findNext().text)
        Curve = float(soup.find(text='Curve ').findNext().text)
        FK_Acc = float(soup.find(text='FK Acc. ').findNext().text)
        Penalties = float(soup.find(text='Penalties ').findNext().text)
        Volleys = float(soup.find(text='Volleys ').findNext().text)
        Shooting = np.mean([Heading,Shot_Power,Finishing,Long_Shots,Curve,FK_Acc,Penalties,Volleys])

        GK_Positioning = float(soup.find(text='GK Positioning ').findNext().text)
        GK_Diving = float(soup.find(text='GK Diving ').findNext().text)
        GK_Handling = float(soup.find(text='GK Handling ').findNext().text)
        GK_Kicking = float(soup.find(text='GK Kicking ').findNext().text)
        GK_Reflexes = float(soup.find(text='GK Reflexes ').findNext().text)
        Goalkeeping = np.mean([GK_Positioning,GK_Diving,GK_Handling,GK_Kicking,GK_Reflexes])

        Description_Player = soup.find_all('div', class_="card mb-5")[0].find_all('div', class_="card-body")[0].text.replace('\n', '')


        player_dict = {'Player': [Player], 'Country': [Country], 'Club': Club, 'Overall_Score': [Overall_Score], 
                      'Potential_Score': [Potential_Score], 'Market_Value': [Market_Value], 'Weekly_Salary': [Weekly_Salary], 
                      'Height': [Height], 'Weight': [Weight], 'Age': [Age], 
                      'Preferred_Foot': [Preferred_Foot], 'Position': [Position_Arr], 'Player_Work_Rate': [Player_Work_Rate],
                      'Kit_Number': [Kit_Number], 'Joined_Club': [Joined_Club], 'Contract_Length': Contract_Length,
                      'Ball_Skills': [Ball_Skills], 'Defence': [Defence], 
                      'Mental': [Mental], 'Passing': [Passing], 'Physical': [Physical], 
                      'Shooting': [Shooting], 'Goalkeeping': [Goalkeeping], 'Description_Player': [Description_Player]}
       
                                             
        df_player = pd.DataFrame(player_dict)
        player_df_json = df_player.to_json()
        channel.basic_publish(exchange='', routing_key='hello', body=player_df_json)
        print(" [x] Sent players's data successfully!")
        # player_list = player_list.append(df_player, ignore_index=True)
    
    pageno += 1
    # if i == 2:
    # 	break
    # # break

player_df = player_list.to_json()
# print(string_playlist_data)
# PRINT RESULT DATAFRAME PLAYER CRAWLED
# print(player_list)


#=============#

# credentials = pika.PlainCredentials('score', 'score')
# connection = pika.BlockingConnection(pika.ConnectionParameters(host='18.179.142.124', credentials=credentials))
# channel = connection.channel()

# channel.queue_declare (queue = 'hello' )

# channel.basic_publish(exchange='', routing_key='hello', body=player_df)
# print(" [x] Sent players's data successfully!'")

connection.close()