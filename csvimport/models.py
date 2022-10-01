from email.errors import CharsetError
from django.db import models

# Create your models here.
class Products(models.Model):
    product_id = models.IntegerField()
    product_name = models.CharField(max_length=100)
    product_price = models.FloatField()
    product_quantity = models.FloatField()

class Carts(models.Model):
    product_id = models.IntegerField()
    customer_name = models.CharField(max_length=100)
    purchase_quantity = models.FloatField()

