#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.ml.linalg import  Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row
from pyspark.ml.feature import StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml.recommendation import ALS


# In[2]:


class DataFrameFilter(object):
    
    @staticmethod
    def Filter(df):
        output = df.spark.sql('select sid,cid,GPA,Grade from '+df.name+' where GPA > 0')
        n_courses_df = output         .groupby('sid')         .count()
        
        output = output         .join(n_courses_df, on='sid', how='inner')         .filter('count > 6')         .drop('count')
        
        n_students_df = output         .groupby('cid')         .count()         .withColumnRenamed('cid', 'cid')
        output= output         .withColumnRenamed('cid', 'cid')         .join(n_students_df, on='cid', how='inner')         .filter('count > 20')         .drop('count')

        output = output         .groupby('sid', 'cid','GPA')         .avg('Grade')         .withColumnRenamed('avg(Grade)', 'Grade')
        output = DataFrame(dataframe=output,name='output',spark=df.spark)
        output.grade_From_Double_To_Int()
        
        return output.dataframe


# In[3]:


########################class DataFrame####################################
class DataFrame(object):
    
    valid_grades = ['A','A-','B+','B','B-','C+','C','C-','D+','D','D-','F']#,'F1','F2','F3','W']
    valid_grades_int = list(range(0,len(valid_grades)))
    grades_dict = dict(zip(valid_grades, valid_grades_int))
    
    def __init__(self,dataframe=None,name='',spark=None):
        if dataframe!=None and name!='' and spark!=None:
            self.dataframe=dataframe
            self.name=name
            self.spark=spark
            self.updateView()
        else:
            self.dataframe=None
            self.name=None
            self.spark=None
            self.clusterer=None
        
    def openFile(self,path='C:',Type='csv',name='',dropna=False,spark=None):
        if self.spark == None and spark!=None:
            self.spark = spark
            filetypes = {
            'csv':self.spark.read.csv(path,header=True,inferSchema=True),
            'json':self.spark.read.json(path)}
            self.dataframe=filetypes[Type]
            self.name=name
            if dropna:
                self.dropNulls()
            self.updateView()
            self.normalize_grades()
            self.updateView()
    def dropNulls(self):
        self.dataframe=self.dataframe.na.drop(how="all")
        self.updateView()
        
    def normalize_grades(self):#### Selects Valid Grades### and sets the data frame to it
        orstring="Grade = "
        for i in range(0,len(self.valid_grades)):
            nstring=" or "+"Grade = "
            orstring +="'"+self.valid_grades[i]+"'"
            if i+1!=len(self.valid_grades):
                orstring+=nstring
        self.dataframe=self.spark.sql("select * from "+self.name+" where "+orstring)
        self.updateView()
        
    def grade_From_String_to_int(self,df):###### gives each grade a score############
        udfstring_to_int=f.udf(DataFrame.string_to_int,IntegerType())
        self.dataframe = self.dataframe.withColumn("Grade",udfstring_to_int("Grade"))
        self.updateView()
        
    @staticmethod
    def string_to_int(x):#######################Not to be played with######################################
        if DataFrame.valid_grades_int[0]==0:
            DataFrame.valid_grades_int.reverse()
        DataFrame.grades_dict = dict(zip(DataFrame.valid_grades,DataFrame.valid_grades_int))
        return DataFrame.grades_dict[x]
    
    def grade_From_Double_To_Int(self):########################## use after vector Assembler
        udfdouble_to_int=f.udf(DataFrame.double_to_int,IntegerType())
        self.dataframe = self.dataframe.withColumn("Grade",udfdouble_to_int("Grade"))
        self.updateView()
        
    @staticmethod
    def double_to_int(x):#calls double to _int best use after vector assembler
        if DataFrame.valid_grades_int[0]==0:
            DataFrame.valid_grades_int.reverse()
        DataFrame.grades_dict = dict(zip(DataFrame.valid_grades, DataFrame.valid_grades_int))
        return int(round(x))
    
    def grades_From_int_to_String(self):##################### grades_from_int_to_string###################
        udfint_to_str = f.udf(DataFrame.int_to_str, StringType())
        self.dataframe=self.dataframe.withColumn("Grade",udfint_to_str("Grade"))
        #self.renameColumn("predicted Grade","Grade")
        self.dataframe=self.dataframe.select('sid','cid','GPA','Grade')
        self.updateView()
        
    @staticmethod
    def int_to_str(x):###################################not to be played with#########################3
        if DataFrame.valid_grades_int[0]==0:
            DataFrame.valid_grades_int.reverse()
        DataFrame.grades_dict = dict(zip(DataFrame.valid_grades_int,DataFrame.valid_grades))
        return DataFrame.grades_dict[x]
        
    def renameColumn(self,cname,newName): ### Column Name(Key) and its equivelent(Value):
            self.dataframe=self.dataframe.withColumnRenamed(cname,newName)
            self.updateView()
            
    def updateView(self):
        self.dataframe.createOrReplaceTempView(self.name)
        
    def createView(self,name,df):
        df.createOrReplaceTempView(name)
        
    def show(self,limit=10):
        self.dataframe.limit(limit).show()


# In[12]:


class GradePredict(object):
    def grade_From_Double_To_Int(self,df):########################## use after vector Assembler
        udfdouble_to_int=f.udf(GradePredict.double_to_int,IntegerType())
        df = df.withColumn("Grade",udfdouble_to_int("Grade"))
        return df
    @staticmethod
    def double_to_int(x):#calls double to _int best use after vector assembler
        valid_grades = ['A','A-','B+','B','B-','C+','C','C-','D+','D','D-','F']#,'F1','F2','F3','W']\
        valid_grades_int = list(range(0,len(valid_grades)))
        if valid_grades_int[0]==0:
            valid_grades_int.reverse()
        grades_dict = dict(zip(valid_grades, valid_grades_int))
        return int(round(x))
    @staticmethod
    def int_to_str(x):
        valid_grades = ['A','A-','B+','B','B-','C+','C','C-','D+','D','D-','F']#,'F1','F2','F3','W']\
        valid_grades_int = list(range(0,len(valid_grades)))
        grades_dict = dict(zip(valid_grades, valid_grades_int))
        if valid_grades_int[0]==0:
            valid_grades_int.reverse()
        grades_dict = dict(zip(valid_grades_int,valid_grades))
        return grades_dict[x]
    def grades_From_int_to_String(self,df):
        udfint_to_str = f.udf(GradePredict.int_to_str, StringType())
        df=df.withColumn("Grade",udfint_to_str("Grade"))
        return df
    def Predict(self,courses=list(),sid=None,GPA=None,spark=None,prediction='ALS'):
        predc=0
        if len(courses)>0 and sid!=None and GPA!=None and GPA!=0 and spark!=None:
            records=list()
            for course in courses:
                records.append((int(sid),int(course),float(GPA)))
            rdd = spark.sparkContext.parallelize(records)
            records = rdd.map(lambda x: Row(sid=int(x[0]), cid = int(x[1]),GPA=float(x[2])))
            records = spark.createDataFrame(records)
            if prediction =='RF':
                predc = self.RandomForestPredict(records,spark)
            elif prediction =='ALS':
                predc = self.ALSPredict(records,spark)
            else:
                predc=0
            return predc
    def ALSPredict(self,df,spark):
        """ must get the Grade columns in integer Representation
        this function takes a data frame that contains the sid,cid and predicts the Grade""" 
        predc = PredictionModels.ALSmodel.transform(df)
        predc = predc.withColumnRenamed('prediction','Grade')
        predc = self.grade_From_Double_To_Int(predc)
        predc = self.grades_From_int_to_String(predc)
        predc=predc.withColumnRenamed('Grade','Predicted Grade')
        #predc = predc.dataframe
        count = predc.count()
        predc2=predc
        predc = predc.take(count)
        spark.catalog.clearCache()
        df.unpersist()
        predc2.unpersist()
        predcRows = list()
        for i in range(0,count):
            predcRows.append(GradePredict.Row_Tuple(predc[i]))
        return predcRows
    def RandomForestPredict(self,df,spark):
        
        """ must get the Grade columns in integer Representation
        this function takes a data frame that contains the sid,cid,GPA and predicts the Grade"""
        ##########################Efred en Dah Course Gded(problem to be thought of) ############################
        assembler = VectorAssembler(inputCols=['cid','GPA'],outputCol='features')
        output = assembler.transform(df)
        predc = PredictionModels.RFmodel.transform(output)
        predc = predc.withColumnRenamed('prediction','Grade')
        predc = self.grade_From_Double_To_Int(predc)
        predc = self.grades_From_int_to_String(predc)
        predc=predc.withColumnRenamed('Grade','Predicted Grade')
        #predc = predc.withColumnRenamed('prediction','Grade')
        #predc = DataFrame(dataframe=predc,name='predictions',spark=spark)#####Reduce time by not creating this obj
        #predc.grades_From_int_to_String()
        #predc.renameColumn('Grade','Predicted Grade')
        #predc=predc.dataframe
        count = predc.count()
        predc2=predc
        predc = predc.take(count)
        predc2.unpersist()
        spark.catalog.clearCache()
        df.unpersist()
        predcRows=list()
        for i in range(0,count):
            predcRows.append(GradePredict.Row_Tuple(predc[i]))
        return predcRows
    @staticmethod
    def Row_Tuple(row):
        """ Takes a row from data frame and returns it's values as a tuple"""
        tupl=list()
        for item in row:
            tupl.append(item)
        return tuple(tupl)
    @staticmethod
    def Cluster(df):
        #output=DataFrameFilter.Filter(df)
        output=df.dataframe
        ouput = output         .groupby('sid')         .agg(f.collect_set('cid').alias('courses'))         .withColumn('n_courses', f.size('courses'))         .filter('n_courses > 15')         .select('sid', f.explode('courses').alias('cid'))
        output = output         .withColumn('one', f.lit(1))         .toPandas()         .pivot_table(index='cid', columns=['sid'], values='one', fill_value=0)
        output = df.spark.createDataFrame(output.reset_index())
        assembler = VectorAssembler(inputCols=output.drop('cid').columns, outputCol="features")
        clustering_df = assembler.transform(output).select('cid', 'features')
        clustered = PredictionModels.Kmodel.transform(clustering_df).select("cid","prediction")
        df.dataframe= df.dataframe.join(clustered,df.dataframe['cid'] == clustered['cid'], how='inner')
        return df
    


# In[5]:


class PredictionModels(object):   
    @staticmethod
    def Train(df,modelName='ALS',faculties=9):
        ###########cluster##############################
        #PredictionModels.TrainKmodel(df,faculties)
        #GradePredict.Cluster(df)
        if modelName == 'RF' or modelName == 'ALL':
            PredictionModels.spark=df.spark
            #output=DataFrameFilter.Filter(df)
            output=df.dataframe
            assembler=VectorAssembler(inputCols=['cid','GPA'],outputCol='features')
            output=assembler.transform(output)
            traind,testd = output.randomSplit([0.7,0.3])
            maxx=-99999999
            trees = 0
            for i in range(50,51):#############To Be Changed Before Deployment###################
                rfc=RandomForestClassifier(labelCol='Grade',featuresCol='features',numTrees=i)
                PredictionModels.RFmodel = rfc.fit(traind)
                #PredictionModels.RFmodel.save('rf_model')
                PredictionModels.RFpreds = PredictionModels.RFmodel.transform(testd)
                PredictionModels.accuracy = PredictionModels.Accuracy(modelName)
                if PredictionModels.accuracy > maxx:
                    maxx = PredictionModels.accuracy
                    trees=i
            #print("Number of Trees that Increase Accuracy of classification is {0}".format(trees))
            #print("With Accuracy "+str(self.accuracy))
        if modelName =='ALS' or modelName == 'ALL':
            rank = 20  # number of latent factors
            #maxIter = 10
            #regParam=0.01  # prevent overfitting
            #output = DataFrameFilter.Filter(df)
            output=df.dataframe
            traind,testd = output.randomSplit([0.7,0.3])
            als = ALS(userCol="sid", itemCol="cid", ratingCol="Grade", 
              rank=rank, 
              coldStartStrategy="drop",
              seed=12)
            
            PredictionModels.ALSmodel = als.fit(traind)
            #PredictionModels.ALSmodel.userFactors.toPandas().to_json("C:\\uf_model.json")
            #PredictionModels.ALSmodel.itemFactors.toPandas().to_json("C:\\if_model.json")
            PredictionModels.ALSpreds = PredictionModels.ALSmodel.transform(testd)
            #PredictionModels.ALSpreds.show()
            PredictionModels.Accuracy(PredictionModels.ALSpreds)
    @staticmethod
    def TrainKmodel(df,faculties):          
        #output=DataFrameFilter.Filter(df)
        output=df.dataframe
        ouput = output         .groupby('sid')         .agg(f.collect_set('cid').alias('courses'))         .withColumn('n_courses', f.size('courses'))         .filter('n_courses > 15')         .select('sid', f.explode('courses').alias('cid'))
        output = output         .withColumn('one', f.lit(1))         .toPandas()         .pivot_table(index='cid', columns=['sid'], values='one', fill_value=0)
        output = df.spark.createDataFrame(output.reset_index())
        assembler = VectorAssembler(inputCols=output.drop('cid').columns, outputCol="features")
        clustering_df = assembler.transform(output).select('cid', 'features')
        kmeans = KMeans(featuresCol='features').setK(faculties).setSeed(1)
        PredictionModels.Kmodel = kmeans.fit(clustering_df)
    @staticmethod
    def Accuracy(modelName='ALS'):
        if modelName =='RF' or modelName=='ALL':
            evaluator=MulticlassClassificationEvaluator(labelCol='Grade',predictionCol='prediction',metricName='accuracy')
            print(PredictionModels.RFmodel.featureImportances)
            return evaluator.evaluate(PredictionModels.RFpreds)
        if modelName=='ALS' or modelName=='ALL':
            evaluator=RegressionEvaluator(metricName='rmse',labelCol='Grade',predictionCol='prediction')
            rmse = evaluator.evaluate(PredictionModels.ALSpreds)
            print('rmse is '+str(rmse))
        


# In[6]:


valid_grades = ['A','A-','B+','B','B-','C+','C','C-','D+','D','D-','F']#,'F1','F2','F3','W']\
valid_grades_int = list(range(0,len(valid_grades)))
grades_dict = dict(zip(valid_grades, valid_grades_int))
        
def grade_From_String_to_int(df):###### gives each grade a score############
    udfstring_to_int=f.udf(string_to_int,IntegerType())
    df = df.withColumn("Grade",udfstring_to_int("Grade"))
    return df

def string_to_int(x):#######################Not to be played with######################################
    if valid_grades_int[0]==0:
        valid_grades_int.reverse()
    grades_dict = dict(zip(valid_grades,valid_grades_int))
    return grades_dict[x]
def int_to_str(x):
    if valid_grades_int[0]==0:
        valid_grades_int.reverse()
    grades_dict = dict(zip(valid_grades_int,valid_grades))
    return grades_dict[x]
def grades_From_int_to_String(df):
    udfint_to_str = f.udf(int_to_str, StringType())
    df=df.withColumn("Grade",udfint_to_str("Grade"))
    return df


# In[7]:


spark = SparkSession.builder.appName('MSA Recommender System').getOrCreate() 
df = DataFrame()
df.openFile(path="C:\\Users\\Mostafa\\Desktop\\data.csv",Type='csv',name='students',dropna=True,spark=spark)
df2 = DataFrame()
df2.openFile(path="C:\\Users\\Mostafa\\Desktop\\data.csv",Type='csv',name='students',dropna=True,spark=spark)
df2.dataframe=grade_From_String_to_int(df2.dataframe)
t=df2
t.dataframe=t.dataframe.select('sid','cid','Grade','GPA').sort(f.col('sid').asc())
t.dataframe=t.dataframe.dropDuplicates()
PredictionModels.Train(t,modelName='ALL')


# In[20]:


df = DataFrame()
df.openFile(path="C:\\Users\\Mostafa\\Desktop\\data.csv",Type='csv',name='students',dropna=True,spark=spark)


# In[21]:


import pyrebase
import random
df=df.dataframe.select('sid','cid','GPA','Grade').filter('GPA>2.1').sort(f.col('sid').asc())
df.show()
cs=df.select('cid')
cs=cs.dropDuplicates()
cscount=cs.count()
cs=cs.collect()
corses=list()
for i in range(0,cscount):
    corses.append(cs[i][0])
count = df.count()
gp=GradePredict()
dff=df
df=df.collect()
spark.catalog.clearCache()
dff.unpersist()
predc=0
url="https://univeristyregisteration.firebaseio.com/"
config = {
  "apiKey": "AIzaSyCZHPmc5GWjKsR5xEXe3gT_PCc6NlnmzQQ",
  "authDomain": "669679351321.firebaseapp.com",
  "databaseURL": url,
  "storageBucket": "univeristyregisteration.appspot.com"
}
availablect=len(corses)-1
scourses=list()
scourses2=list()
sid=165
gpa=df[0][2]
student = dict()
courses= ['MTH',"CS","ECE","MNG","ENG","SE","MCOM"]
names=['Ahmed','Laila','Ehab','Mohsen','Nouran','Tarek','Dareen']
firebase = pyrebase.initialize_app(config)
db = firebase.database()
not_taken=list()
cttt=0
for i in range(0,count):
    cindex=random.randint(0,len(courses)-1)
    if df[i][0]!=sid and cttt==0:
        sid=df[i][0]
        cttt+=1
    if df[i][0]!=sid:
        ct=0
        not_taken=list()
        untaken=corses[random.randint(0,availablect)]
        while ct!=7:
            while True:
                if untaken in scourses2:
                    untaken=corses[random.randint(0,availablect)]
                else:
                    scourses2.append(untaken)
                    break
            not_taken.append(untaken)
            ct+=1
        print(str(not_taken))
        preds=gp.Predict(courses=not_taken,sid=sid,GPA=gpa,spark=spark)
        if preds==None or preds==0:
            scourses=list()
            scourses2=list()
            sid=df[i][0]
            gpa=df[i][2]
            continue
        preds2=list()
        if len(preds)<len(not_taken):
            cmps=list()
            for pred in preds:
                if len(pred)==4:
                    sid2,cid2,gpa2,grade2=pred
                    cmps.append(cid2)
            for ccc in not_taken:
                 if ccc not in cmps:
                    preds2.append(ccc)
            preds.extend(gp.Predict(courses=preds2,sid=sid,GPA=gpa,spark=spark,prediction='RF'))
        print(str(preds))
        exp=list()
        for pred in preds:
            cindex=random.randint(0,len(courses)-1)
            if len(pred)==4:
                sid2,cid2,gpa2,grade2=pred
                data2={
                   "name":courses[cindex] +" "+str(cid2),
                    "id":str(cid2),
                    "grade":"",
                    "expGrade":grade2,
                    "isOpen":"true"
                    }
                exp.append(data2)
        #print(str(exp))
        student['student']={
            "id":sid,
            "name":names[cindex],
            "courses":scourses,
            "expcourses":exp,
            "GPA":gpa
        }
        print(str(student['student'])+"\n")
        results=db.child('student').child(sid).push(student['student'])
        #for kk in scourses:
        #    results = db.child('student').child(sid).child('courses').child(kk['id']).push(kk)
        #    resulst = db.child('student').child(sid).child('courses').child(kk['id']).child('Grade').push(kk['grade'])
        #    resulst = db.child('student').child(sid).child('courses').child(kk['id']).child('Expgrade').push("")
        #for kk in exp:
        #    results = db.child('student').child(sid).child('expcourses').child(kk['id']).push(kk)
        #    results = db.child('student').child(sid).child('expcourses').child(kk['id']).child('Expgrade').push(kk['expGrade'])
        scourses=list()
        scourses2=list()
        sid=df[i][0]
        gpa=df[i][2]
        ####push
    data={
       "name":courses[cindex] +" "+str(df[i][1]),
        "id":str(df[i][1]),
        "grade":str(df[i][3]),
        "expGrade":""
        }
    scourses2.append(df[i][1])
    scourses.append(data)


# In[ ]:





# In[ ]:


#PredictionModels.Train(df,modelName='ALL')


# In[ ]:


#def load_model():
#    model = RandomForestClassifier.load("C:\\Users\\Mostafa\\Desktop\\abc\\rf_model")
#    model.transform(df.dataframe).show()


# In[ ]:


#load_model()


# In[ ]:


#gp = GradePredict()
#courses=[47,44,49]    #courses=[138,234,88,352,276]
#################################sid=69
#################################gpa=2.6
#sid=1195
#gpa=2.6
#preds=gp.Predict(courses=courses,sid=sid,GPA=gpa,spark=spark)
#preds2=list()
#if len(preds)<len(courses):
#    cmps=list()
#    for pred in preds:
#        sid,cid,gpa,grade=pred
#        cmps.append(cid)
#    for course in courses:
#         if course not in cmps:
#            preds2.append(course)
#preds.extend(gp.Predict(courses=preds2,sid=sid,GPA=gpa,spark=spark,prediction='RF'))
#print(str(preds))
####return preds


# In[ ]:


#PredictionModels.TrainKmodel(df,9)


# In[ ]:


#PredictionModels.Cluster(df)


# 
