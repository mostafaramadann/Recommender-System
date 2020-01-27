#!/usr/bin/env python
# coding: utf-8

# In[10]:


from pyspark.sql import SparkSession
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


# In[11]:


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


# In[12]:


###################Class Insights######################################
class Insights(object):
    
    def __init__(self,name,spark):
        self.name=name
        self.spark=spark
        
    def showInsight(self,sqlstatement,no_records=10):
        if sqlstatement!='':
            self.spark.sql(sqlstatement).limit(no_records).show()

    def showTopStudents_Fac(self,faculty='',no_records=10):###########This function is to be implemented after clustering##
        print("Under Construction")
        
    def showTopStudents(self,no_records=10):###########This function is to be implemented after clustering####
        self.spark.sql("select * from "+self.name+" order by GPA desc").limit(no_records).show()
        
 #   def showTopTakenCourses(self,faculty='',no_records=10):###########This function is to be implemented after clustering
 #       print("Under Construction")
    
 #   def showSemesterInsight(self,semester=0):###################
 #       print("UnderConstruction")
        
#    def Visualize(self,dataframe):#####################
#        print("UnderConstruction")


# In[13]:


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
            self.insights=Insights(self.name,self.spark)
            #self.clusterer=Clusterer(self)
            self.updateView()
        else:
            self.dataframe=None
            self.name=None
            self.spark=None
            self.insights=None
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
            self.insights=Insights(self.name,self.spark)
            self.normalize_grades()
           # self.clusterer=Clusterer(self)
            self.updateView()
   # def cluster(self):
   #     self.clusterer.Fac_cluster()      
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
        
    def grade_From_String_to_int(self):###### gives each grade a score############
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


# In[22]:


class GradePredict(object):
        
    def Predict(self,courses=list(),sid=None,GPA=None,spark=None,prediction='ALS'):
        
        if len(courses)>0 and sid!=None and GPA!=None and GPA!=0 and spark!=None:
            records=list()
            for course in courses:
                records.append((int(sid),int(course),float(GPA)))
            rdd = spark.sparkContext.parallelize(records)
            records = rdd.map(lambda x: Row(sid=int(x[0]), cid = int(x[1]),GPA=float(x[2])))
            records = spark.createDataFrame(records)
            #records.show()
            if prediction =='RF':
                predc = self.RandomForestPredict(records)
            elif prediction =='ALS':
                predc = self.ALSPredict(records)
        if predc!=None:
            return predc
        else:
            return None
    def ALSPredict(self,df):
        """ must get the Grade columns in integer Representation
        this function takes a data frame that contains the sid,cid and predicts the Grade""" 
        predc = PredictionModels.ALSmodel.transform(df)
        predc.show()
        predc = predc.withColumnRenamed('prediction','Grade')
        predc = DataFrame(dataframe=predc,name='predictions',spark=spark)
        predc.grade_From_Double_To_Int()
        predc.show()
        predc.grades_From_int_to_String()
        predc.show()
        predc.renameColumn('Grade','Predicted Grade')
        predc.show()
        predc = predc.dataframe
        count = predc.count()
        predc = predc.collect()
        predcRows = list()
        for i in range(0,count):
            predcRows.append(GradePredict.Row_Tuple(predc[i]))
        return predcRows
    def RandomForestPredict(self,df):
        
        """ must get the Grade columns in integer Representation
        this function takes a data frame that contains the sid,cid,GPA and predicts the Grade"""
        ##########################Efred en Dah Course Gded(problem to be thought of) ############################
        assembler = VectorAssembler(inputCols=['cid','GPA'],outputCol='features')
        output = assembler.transform(df)
        predc = PredictionModels.RFmodel.transform(output)
        predc = predc.withColumnRenamed('prediction','Grade')
        predc = DataFrame(dataframe=predc,name='predictions',spark=self.spark)
        predc.grades_From_int_to_String()
        predc.renameColumn('Grade','Predicted Grade')
        predc.show()
        predc=predc.dataframe
        count = predc.count()
        predc = predc.collect()
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
    


# In[15]:


class PredictionModels(object):   
    @staticmethod
    def Train(df,modelName='ALS'):
        if modelName == 'RF' or modelName == 'ALL':
            PredictionModels.spark=df.spark
            output=DataFrameFilter.Filter(df)
            assembler=VectorAssembler(inputCols=['cid','GPA'],outputCol='features')
            output=assembler.transform(output)
            traind,testd = output.randomSplit([0.7,0.3])
            maxx=-99999999
            trees = 0
            for i in range(50,51):#############To Be Changed Before Deployment###################
                rfc=RandomForestClassifier(labelCol='Grade',featuresCol='features',numTrees=i)
                PredictionModels.RFmodel = rfc.fit(traind)
                PredictionModels.RFpreds = PredictionModels.RFmodel.transform(testd)
                PredictionModels.RFpreds.show()
                PredictionModels.accuracy = PredictionModels.Accuracy(modelName)
                if PredicitionModels.accuracy > maxx:
                    maxx = PredictionModels.accuracy
                    trees=i
            #print("Number of Trees that Increase Accuracy of classification is {0}".format(trees))
            #print("With Accuracy "+str(self.accuracy))
        if modelName =='ALS' or modelName == 'ALL':
            rank = 20  # number of latent factors
            #maxIter = 10
            #regParam=0.01  # prevent overfitting
            output = DataFrameFilter.Filter(df)
            traind,testd = output.randomSplit([0.8,0.2])
            als = ALS(userCol="sid", itemCol="cid", ratingCol="Grade", 
              rank=rank, 
              coldStartStrategy="drop",
              seed=12)
            PredictionModels.ALSmodel = als.fit(traind)
            PredictionModels.ALSpreds = PredictionModels.ALSmodel.transform(testd)
            PredictionModels.ALSpreds.show()
            PredictionModels.Accuracy(PredictionModels.ALSpreds)
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
        


# In[16]:


spark = SparkSession.builder.appName('MSA Recommender System').getOrCreate() 
df = DataFrame()
df.openFile(path="C:\\Users\\Mostafa\\Desktop\\data.csv",Type='csv',name='students',dropna=True,spark=spark)
#df.show()


# In[17]:


df.renameColumn(df.dataframe.columns[0],'sid')
df.renameColumn(df.dataframe.columns[1],'cid')
df.grade_From_String_to_int()
#df.show(5)


# In[18]:


PredictionModels.Train(df,modelName='ALS')


# In[23]:


gp = GradePredict()
courses=[135,243]
sid=463
gpa=2.6
gp.Predict(courses=courses,sid=sid,GPA=gpa,spark=spark)


# 
