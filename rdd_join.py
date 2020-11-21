from pyspark import SparkConf,SparkContext
conf = SparkConf().setAppName("RDD-JOIN").setMaster("local[4]")
sc = SparkContext(conf=conf)

order_items_rdd = sc.textFile("/Users/mehmeteraysurmeli/Downloads/order_items.txt")
order_items_rdd = order_items_rdd.filter(lambda x : "orderItemName" not in x)
# print(order_items_rdd.take(20))

products_rdd = sc.textFile("/Users/mehmeteraysurmeli/Downloads/products.txt")
products_rdd = products_rdd.filter(lambda x : "productId" not in x)
# print(products_rdd.take(20))

def make_products_pair_rdd(line):
    productId = line.split(",")[0]
    productCategoryId = line.split(",")[1]
    productName = line.split(",")[2]
    productDescription = line.split(",")[3]
    productPrice = line.split(",")[4]
    productImage = line.split(",")[5]
    return (productId , (productCategoryId,productName,productDescription,productPrice,productImage))
products_pair_rdd = products_rdd.map(make_products_pair_rdd)
# print(products_pair_rdd.take(20))

def make_order_items_pair_rdd(line):
    orderItemName = line.split(",")[0]
    orderItemOrderId = line.split(",")[1]
    orderItemProductId = line.split(",")[2]
    orderItemQuantity = line.split(",")[3]
    orderItemSubTotal = line.split(",")[4]
    orderItemProductPrice = line.split(",")[5]

    return (orderItemOrderId, (orderItemName, orderItemProductId, orderItemQuantity, orderItemSubTotal, orderItemProductPrice))
order_items_pair_rdd = order_items_rdd.map(make_order_items_pair_rdd)
# print(order_items_rdd.take(20))

order_items_products_pair_rdd = order_items_pair_rdd.join(products_pair_rdd)
# print(order_items_products_pair_rdd.take(5))

print(products_pair_rdd.count())
print(order_items_pair_rdd.count())
print(order_items_products_pair_rdd.count())