from django.shortcuts import render
from yaml import serialize
from .serializers import ProductSerializer,CartSerializer
from .models import Products,Carts
from rest_framework import viewsets
from rest_framework.response import Response
import csv,json,io
from django.core.files.base import ContentFile
from django.core.files.storage import FileSystemStorage
from rest_framework.decorators import action
import pyspark
from pyspark.sql import SparkSession
from django.http import HttpResponse
import pandas as pd
import zipfile
from wsgiref.util import FileWrapper
from django.http import StreamingHttpResponse
from zipfile import ZipFile
from pyspark import SparkContext
from pyspark.sql import SQLContext

fs = FileSystemStorage(location = 'tmp/')

# Create your views here.
class Csvimport(viewsets.ModelViewSet):
        queryset = Products.objects.all()
        serializer_class = ProductSerializer
        
        @action(detail=False,methods=['GET'])
        def upload_data(self,request):
            files = request.FILES.getlist("files")
            joincolumn1 = request.data['joincolumn1']
            sc = SparkContext('local','example')  # if using locally
            sql_sc = SQLContext(sc)
            file1 = files[0]
            file2 = files[1]
            
            #first csv
            product_content = file1.read()

            product_file_content =ContentFile(product_content)

            product_file_name = fs.save(
                "tmpfile1.csv", product_file_content
            )
            product_tmp_file = fs.path(product_file_name)

            Spark_Full1 = sc.emptyRDD()
            chunk_100k1 = pd.read_csv(product_tmp_file, chunksize=100000)

            headers1 = list(pd.read_csv(product_tmp_file, nrows=0).columns)

            for chunky in chunk_100k1:
                Spark_Full1 +=  sc.parallelize(chunky.values.tolist())

            readfile1 = Spark_Full1.toDF(headers1)

            readfile1.show()


            #seconf csv
            product_content2 = file2.read()

            product_file_content2 =ContentFile(product_content2)

            product_file_name2 = fs.save(
                "tmpfile2.csv", product_file_content2
            )
            product_tmp_file2 = fs.path(product_file_name2)

            Spark_Full2 = sc.emptyRDD()
            chunk_100k2 = pd.read_csv(product_tmp_file2, chunksize=100000)

            headers2 = list(pd.read_csv(product_tmp_file2, nrows=0).columns)

            for chunky in chunk_100k2:
                Spark_Full2 +=  sc.parallelize(chunky.values.tolist())

            readfile2 = Spark_Full2.toDF(headers2)

            readfile2.show()

            #joints
            joined = readfile1.join(readfile2, joincolumn1,"inner")
            nonproductjoined = readfile1.join(readfile2, joincolumn1,"leftanti")
            noncartjoined = readfile2.join(readfile1, joincolumn1,"leftanti")
            
            joined.write.mode("overwrite").csv("output/joined.csv")
            nonproductjoined.write.mode("overwrite").csv("output//nonproductjoined.csv")
            noncartjoined.write.mode("overwrite").csv("output/noncartjoined.csv")

            return Response("succesfully exported")
            
