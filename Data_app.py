
#EDA
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

class Data():
    catelist=pd.read_csv('Files/category__.csv')
    local_engine = create_engine('mysql+pymysql://chencheng:iKWz@4*7W55@10.1.1.202:3306')
    cloud_engine = create_engine('mysql+pymysql://root:aukey@888@139.9.201.134:3306')
    nlp_sql='''select distinct asin from amazon.asin_review'''

    def rank_load_data(category_id=None,month=None):
        rank_sql=f'''
        select category_id,category_name,asin,ranking,snapshotted_at as date
        from mws_data.project_20_listings where category_id = '{category_id}'
        and MONTH(snapshotted_at) = {month}
        ''' if category_id and month else '''
        select asin,category_id,category_name,ranking, snapshotted_at as date from mws_data.project_20_listings
        '''
        data=pd.read_sql(rank_sql,Data.local_engine) 
        data['date']=pd.to_datetime(pd.to_datetime(data['date'])).dt.date
        data['date']=data['date'].apply(lambda x: str(x).replace('2021-',''))
        return data
    
    

    def nlp_asins():
        asins = pd.read_sql(Data.nlp_sql,Data.cloud_engine)        
        return asins

    def nlp_data(chosedasin):
        review_sql=f'''
        select review_star_rating,review_info, right(review_date,17) 
        as review_date from amazon.asin_review 
        where asin='{chosedasin}'
        '''  
        review = pd.read_sql(review_sql,Data.cloud_engine)
        return review
