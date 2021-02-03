#Core
import uvicorn
import json
from fastapi import FastAPI
from typing import Optional
from starlette.middleware.cors import CORSMiddleware

#EDA
import time
import pandas as pd
import numpy as np
import plotly_express as px
from Data_app import Data



class Rank(Data):
    def category_data(category): #计算日排名,存入缓存
        category_id = Rank.catelist[Rank.catelist['category_name']==category]['category_id'].values[0]
        test=[]
        colnames={}
        data=Rank.rank_load_data(category_id)
        table=data.groupby(['asin','date'])['ranking'].agg('median').apply(lambda x:int(x)).unstack(level=1).reset_index(level=0)
        table['当月排名']=table.median(axis=1).astype(int)
        table=table.sort_values(by=table.columns[-1])
        for i in table.columns:
            colnames['title']=i.upper()
            colnames['key']=i
            colnames['width']=100
            test.append(colnames)
            colnames={}
        test[0]['fixed']='left'
        test[-1]['fixed']='right'
        result=table.to_json(orient = 'records', force_ascii=False,date_format='iso')
        catdata = result
        colnames= json.dumps(test)
        return catdata,colnames

    def asin_data(table,chosed_asin):
        asin_raw_rank=pd.DataFrame(table.loc[f'{chosed_asin}',:].dropna()).transpose()
        asin_month_rank=asin_raw_rank.apply(lambda x: int(np.median(x)),axis=1)[0]
        table_chosed_asin=pd.DataFrame(table.loc[f'{chosed_asin}'])
        table_chosed_asin['date']=table_chosed_asin.index.get_level_values(0)
        table_chosed_asin.reset_index(drop=True)
        asin_median_rank=table.apply(lambda x: np.median(x),axis=1).sort_values()
        return table_chosed_asin.to_json(orient='split'), asin_month_rank.to_json(orient='split'), asin_median_rank.to_json(orient='split')


app = FastAPI()

origins = [
    "http://localhost:8080",
]

app.add_middleware(         # 添加中间件
    CORSMiddleware,         # CORS中间件类
    allow_origins=['*'],    # 允许起源
    allow_credentials=True, # 允许凭据
    allow_methods=["*"],    # 允许方法
    allow_headers=["*"],    # 允许头部
)

@app.get('/')
async def root():
    return {'message':'Hello World'}

@app.get('/rank/{category}')
def read_table(category: Optional[str] = None):
    return Rank.category_data(category)

@app.get('/categoryNames')
def get_catlist():
    return Rank.catelist['category_name'].to_json(orient='records')


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

