# JSON2SQL

To convert json data to a sql query.

The plugin takes in the knowledge base and then uses the json to create SQL queries. The basic structure of the json that will be parsed by the plugin is:

```javascript
{
    "where": {
        "field": "<value>",
        "operator": "<operator-choice>",
        "value": "<value>",
        "secondary_value": "<value>" (optional)
    }
}
```

This is the simplest form of input that can be provided. Examples of a more complex json:

```javascript
{
 "and": [{
          "where": {
                "field": "<value>",
                "operator": "<operator-choice>",
                "value": "<value>",
                "secondary_value": "<value>" (optional)
           }
         },
         {
           "not": [{
              "or":[{
                      "not": [{                                         
                               "where": {
                                    "field": "<value>",
                                    "operator": "<operator-choice>",
                                    "value": "<value>",
                                    "secondary_value": "<value>" (optional)
                                }
                             }]
                     },
                     {
                        "where": {
                            "field": "<value>",
                            "operator": "<operator-choice>",
                            "value": "<value>",
                            "secondary_value": "<value>" (optional)
                        }
                     }]
            }]
         }]
    }
}
```

```javascript
{
    "and": [{
          "where": {
                "field": "<value>",
                "operator": "<operator-choice>",
                "value": "<value>",
                "secondary_value": "<value>" (optional)
           }
         },
         {
           "not": [{
              "or":[{
                      "exists": [{                                         
                                   "where": {
                                        "field": "<value>",
                                        "operator": "<operator-choice>",
                                        "value": "<value>",
                                        "secondary_value": "<value>" (optional)
                                    }
                                }]
                     },
                     {
                        "where": {
                            "field": "<value>",
                            "operator": "<operator-choice>",
                            "value": "<value>",
                            "secondary_value": "<value>" (optional)
                        }
                     }]
            }]
         }]
    }
}
```


The knowledge base required for the plugin are two mappings:
 * *base_table*: This provides the name of the table that would be used in **FROM** clause of the SQL.

 * *field_mapping*:
   This mapping is used to get information about the field using the *field_identifier* in the above mentioned JSON. This mapping would in the following format:
   ```python
   (
    ('field_identifier1', 'field_name1', 'table_name1', 'data_type'),
    ('field_identifier2', 'field_name2', 'table_name2', 'data_type')
   )
   ```
   
    where **field_name** and **table_name** belong to the exact names in the DB.
 
 * *paths*: This mapping is used to get models required to join a table with base table. The format for this mapping would be:
    ```python
    (
        ('join_table1', 'join_field1', 'parent_table1', 'parent_field1'),
        ('join_table2', 'join_field2', 'parent_table2', 'parent_field2'),
    )
    ```
    Here **join_table** is the table that has to reach the **base_table** and **parent_table** is the table that is the first step in the path of **join_table** to **base_table**.
    This mapping will be recursive in nature, say, for example, we have a table *user* and the immediate parent of user to reach to the **base_table** is *patient* table. So an entry in the paths mapping would be 
    ```('user', 'id', 'patient', 'user_id')```
    . So here **join_field** is **id** and **parent_field** is **user_id**. These fields represent the columns that would take part in the **INNER JOIN**'s **ON** clause.

    *NOTE*: Every table can have just a single immediate parent. So in the entire mapping the **join_table** field will be unique. Also, there would be no mapping for **base_table** as **join_table**.


## How to use

* import JOSN2SQLGenerator from engine.py in json2sql 
* Initailize a JSON2SQLGenerator with field_mapping and paths
    ```python
        obj = JSON2SQLGenerator(field_mapping, paths)
    ```
* Call the **generate_sql** function
    ```python
       obj.generate_sql(<json_data>, <base_table>) 
    ``` 
    This function will return the SQL query.