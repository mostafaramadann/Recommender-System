from django.shortcuts import render
import json
from django.http import JsonResponse,HttpResponse
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.views.generic import View
from pyspark.ml.classification import RandomForestClassificationModel
import pyspark.sql.functions as f
from pyspark.ml.feature import VectorAssembler
import pandas as pd
from numpy import array
from numpy import dot
from pyspark.sql import SparkSession
# Create your views here.
def loadRFModel(df):
    assembler=VectorAssembler(inputCols=['cid','GPA'],outputCol='features')
    output=assembler.transform(df)
    model = RandomForestClassificationModel.load("rf_model")
    ret = model.transform(output).select('cid','prediction')
    return ret.head(7)

grades=['A','A-','B+','B','B-','C+','C','C-','D+','D','D-','F']
def predict(id):
    gpa=0.0
    courses=list()
    preds = dict()
    if id!=None:
        spark = SparkSession.builder.appName('MSA Recommender System').getOrCreate() 
        df=spark.read.csv('data.csv',header=True,inferSchema=True)
        df2 = df.select('cid')
        df = df.filter(f.col('sid')==id)
        df = df.alias('a')
        df2 = df2.alias('b')
        df= df.join(df2, df['cid'] != df2['cid']).select('a.sid','b.cid','a.GPA')
        df=df.dropDuplicates()
        rows=df.head(7)
        gpa=rows[0][2]
        for i in range(0,7):
            courses.append(rows[i][1])##el mafrood count el courses ely mat5dtsh bs for simplicity han7ot 7  
        i_f = pd.read_json('uf_model.json')
        u_f = pd.read_json('if_model.json')
        i_f.set_index('id',inplace=True)
        u_f.set_index('id',inplace=True)
        #for i in courses:
        #    i_f_i=array(u_f.loc[u_f.index[id]][0])
       #     u_f_i=array(i_f.loc[i_f.index[i]][0])
       #     preds[i]=round(dot(i_f_i,u_f_i))
       # if len(preds)<len(courses):
       #     rows = loadRFModel(df)
       #     for i in rows:
       #         if i[0] not in preds.keys():
        #            preds[i[0]]=i[1]
       # for k in preds:
       #     for i in range(0,len(grades)):
       #         if preds[k] not in grades:
       #             if int(preds[k])==i:
       #                 preds[k]=grades[i]
        return courses

@api_view(['GET', 'POST'])
def getPreds(request,id):
    return HttpResponse(json.dumps(predict(id)))