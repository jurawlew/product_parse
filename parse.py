import threading


class Parse(threading.Thread):

    def __init__(self, client, category_id, product_id, result, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client
        self.category_id = category_id
        self.product_id = product_id
        self.result = result

    def run(self):
        count_attrs = self.client.execute(
            "SELECT Count(pa.attribute_id) as count1, "
            "round((SELECT Count(pa.attribute_id) as count2 "
            "FROM ecomch.categories AS c "
            "JOIN ecomch.category_attrs AS ca ON c.category_id=ca.category_id "
            "LEFT JOIN ecomch.product_attr AS pa ON ca.attribute_id  = pa.attribute_id "
            f"WHERE c.category_id = toString({self.category_id}) AND pa.product_id = toString({self.product_id}) "
            f"AND (empty(pa.value) OR pa.value is NULL)) / count1 *100, 2) "

            f"FROM ecomch.categories AS c "
            "JOIN ecomch.category_attrs AS ca ON c.category_id=ca.category_id "
            "LEFT JOIN ecomch.product_attr AS pa ON ca.attribute_id  = pa.attribute_id "
            f"WHERE c.category_id = toString({self.category_id}) AND pa.product_id = toString({self.product_id})")

        count_symbol_in_attr = self.client.execute(
            "SELECT DISTINCT pa.attribute_id, LENGTH(pa.value) as len "
            "FROM ecomch.categories AS c "
            "JOIN ecomch.category_attrs AS ca ON c.category_id=ca.category_id "
            "LEFT JOIN ecomch.product_attr AS pa ON ca.attribute_id  = pa.attribute_id "
            f"WHERE c.category_id = toString({self.category_id}) AND pa.product_id = toString({self.product_id})")

        count_images_in_attr = self.client.execute(
            "SELECT ca.attribute_name, countMatches(pa.value, ';') + 1 as count "
            "FROM ecomch.categories AS c "
            "JOIN ecomch.category_attrs AS ca ON c.category_id=ca.category_id "
            "LEFT JOIN ecomch.product_attr AS pa ON ca.attribute_id  = pa.attribute_id "
            f"WHERE c.category_id = toString({self.category_id}) AND pa.product_id = toString({self.product_id}) AND ca.attribute_name = 'images'"
            f"GROUP BY ca.attribute_name, pa.value")

        change_str = self.client.execute(
            f"SELECT replaceOne(ca.description, 'n', concat('n\U0001F923\n', pa.value, '\n')) "
            "FROM ecomch.categories AS c "
            "JOIN ecomch.category_attrs AS ca ON c.category_id=ca.category_id "
            "LEFT JOIN ecomch.product_attr AS pa ON ca.attribute_id  = pa.attribute_id "
            f"WHERE c.category_id = toString({self.category_id}) AND pa.product_id = toString({self.product_id}) AND ca.required = TRUE ")

        self.result.extend([count_attrs, count_symbol_in_attr, count_images_in_attr, change_str])