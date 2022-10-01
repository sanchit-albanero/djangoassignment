from .models import Products,Carts
from rest_framework import serializers
class ProductSerializer(serializers.ModelSerializer):
    class Meta:
        model = Products
        fields = ['product_id','product_name','product_price','product_quantity']

class CartSerializer(serializers.ModelSerializer):
    class Meta:
        model = Carts
        fields = ['product_id','customer_name','purchase_quantity']
