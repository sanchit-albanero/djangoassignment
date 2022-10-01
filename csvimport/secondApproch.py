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
            spark = SparkSession.builder.appName("csvimport").getOrCreate()
            product_df = self.productImport(request,spark)
            cart_df = self.cartImport(request,spark)
            joincolumn = request.data['joincolumn']
            # csv_export=self.performJoin(spark)

            # return csv_export
            
            joined = product_df.join(cart_df, joincolumn,"inner")
            nonproductjoined = product_df.join(cart_df, joincolumn,"leftanti")
            noncartjoined = cart_df.join(product_df, joincolumn,"leftanti")
            
            joined.write.csv("joined.csv")
            nonproductjoined.write.csv("nonproductjoined.csv")
            noncartjoined.write.csv("noncartjoined.csv")

            return Response("succesfully exported")

        def productImport(self,request,spark):
            product_dataframes_list = []
            #product csv
            product_file = request.FILES["products"]
            # for file in product_file:
            product_content = product_file.read()

            product_file_content =ContentFile(product_content)

            product_file_name = fs.save(
                "tmpfiles.csv", product_file_content
            )
            product_tmp_file = fs.path(product_file_name)
            product_temp_df = spark.read.csv(product_tmp_file,header='true', inferSchema='true', sep=",")
            product_dataframes_list.append(product_temp_df)

            for dataset in product_dataframes_list:
                jsonData=dataset.toJSON().collect()
                product_list = []
                for data in jsonData:
                    joinedDataJson = json.loads(data)

                    product_list.append(
                        Products(
                            product_id = joinedDataJson["product_id"],
                            product_name = joinedDataJson["product_name"],
                            product_price = joinedDataJson["product_price"],
                            product_quantity = joinedDataJson["product_quantity"]
                        )
                    )
                Products.objects.bulk_create(product_list)
            return product_temp_df

        
        
        def cartImport(self,request,spark):
            cart_dataframes_list = []
            #cart csv
            cart_file = request.FILES["carts"]
            # for file in product_file:
            cart_content = cart_file.read()

            cart_file_content =ContentFile(cart_content)

            cart_file_name = fs.save(
                "tmpfiles.csv", cart_file_content
            )
            cart_tmp_file = fs.path(cart_file_name)
            cart_temp_df = spark.read.csv(cart_tmp_file,header='true', inferSchema='true', sep=",")
            cart_dataframes_list.append(cart_temp_df)

            for dataset in cart_dataframes_list:
                jsonData=dataset.toJSON().collect()
                cart_list = []
                for data in jsonData:
                    cart_joinedDataJson = json.loads(data)
                    print(cart_joinedDataJson)
                    cart_list.append(
                        Carts(
                            product_id = cart_joinedDataJson["product_id"],
                            customer_name = cart_joinedDataJson["customer_name"],
                            purchase_quantity = cart_joinedDataJson["purchase_quantity"]
                        )
                    )
                Carts.objects.bulk_create(cart_list)
            return cart_temp_df
        


        def performJoin(self,spark):
            product = Products.objects.all()
            productserializers = ProductSerializer(product,many=True).data

            productColumns = ["product_id","product_name","product_price","product_quantity"]

            productDF = spark.createDataFrame(data=productserializers, schema = productColumns)
            productDF.printSchema()
            productDF.show(truncate=False)

            cart = Carts.objects.all()
            cartserializers = CartSerializer(cart,many=True).data
            print(cartserializers)
            cartColumns = ["customer_name","product_id","purchase_quantity"]

            cartDF = spark.createDataFrame(data=cartserializers, schema = cartColumns)
            cartDF.printSchema()
            cartDF.show(truncate=False)
            
            productDF.join(cartDF,productDF.product_id ==  cartDF.product_id,"inner") \
                .show(truncate=False)

            joined = productDF.join(cartDF,productDF.product_id ==  cartDF.product_id,"inner")

            notjoinedproducts = productDF.join(cartDF,productDF.product_id ==  cartDF.product_id,"leftanti")

            notjoinedcarts = cartDF.join(productDF,cartDF.product_id ==  productDF.product_id,"leftanti")

            # Create the HttpResponse object with the appropriate CSV header.
            joinedresponse = HttpResponse(content_type='text/csv')
            joinedresponse['Content-Disposition'] = 'attachment; filename="csv_joined_write.csv"'

            joinedresponse = HttpResponse(content_type='text/csv')
            joinedresponse['Content-Disposition'] = 'attachment; filename="csv_joined_write.csv"'

            joinedwriter = csv.writer(joinedresponse)
            joinedwriter.writerow(['product_id', 'product_name', 'product_quantity', 'product_price', 'customer_name', 'purchase_quantity'])

            productresponse = HttpResponse(content_type='text/csv')
            productresponse['Content-Disposition'] = 'attachment; filename="csv_product_write.csv"'

            productresponse = HttpResponse(content_type='text/csv')
            productresponse['Content-Disposition'] = 'attachment; filename="csv_product_write.csv"'

            productwriter = csv.writer(productresponse)
            productwriter.writerow(['product_id', 'product_name', 'product_quantity', 'product_price'])

            cartresponse = HttpResponse(content_type='text/csv')
            cartresponse['Content-Disposition'] = 'attachment; filename="csv_cart_write.csv"'

            cartresponse = HttpResponse(content_type='text/csv')
            cartresponse['Content-Disposition'] = 'attachment; filename="csv_cart_write.csv"'

            cartwriter = csv.writer(cartresponse)
            cartwriter.writerow(['product_id', 'customer_name', 'purchase_quantity'])

            all_data = []
            for joinedData in joined.toJSON().collect():
                joinedDataJson = json.loads(joinedData)
                joinedwriter.writerow([
                    joinedDataJson["product_id"],
                    joinedDataJson["product_name"],
                    joinedDataJson["product_quantity"],
                    joinedDataJson["product_price"],
                    joinedDataJson["customer_name"],
                    joinedDataJson["purchase_quantity"]
                ])

            for joinedData in notjoinedproducts.toJSON().collect():
                joinedDataJson = json.loads(joinedData)
                productwriter.writerow([
                    joinedDataJson["product_id"],
                    joinedDataJson["product_name"],
                    joinedDataJson["product_quantity"],
                    joinedDataJson["product_price"]
                ])


            for joinedData in notjoinedcarts.toJSON().collect():
                joinedDataJson = json.loads(joinedData)
                cartwriter.writerow([
                    joinedDataJson["product_id"],
                    joinedDataJson["customer_name"],
                    joinedDataJson["purchase_quantity"]
                ])
            
            return joinedresponse
            # return productresponse
            # return cartresponse
            # cartresponse.write.csv("state_city_weather_out1.csv")
            
