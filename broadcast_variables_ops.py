from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[4]").setAppName("broadcastvariablesops")
sc = SparkContext(conf=conf)

products_rdd = sc.textFile('/Users/mehmeteraysurmeli/Downloads/products.txt')

products_rdd = products_rdd.filter(lambda x: 'productId' not in x)

# def read_products(line):
#     productId = line.split(",")[0]
#     productName = line.split(",")[2]
#     return (productId,productName)
# products_rdd = products_rdd.map(read_products)
# print(products_rdd.take(20))


def read_products():
    products_text_wrappers = open('/Users/mehmeteraysurmeli/Downloads/products.txt','r',encoding='utf-8')
    products = products_text_wrappers.readlines()
    product_id_name = {}
    for line in products:
        if 'productCategoryId' not in line:
            product_id = int(line.split(",")[0])
            product_name = line.split(",")[2]
            product_id_name.update({product_id:product_name})
    return product_id_name
products = read_products()

broadcast_products = sc.broadcast(products)
print(broadcast_products.value.get(114))
