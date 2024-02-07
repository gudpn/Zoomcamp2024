#!/usr/bin/env python
# coding: utf-8

# # **Homework**: Data talks club data engineering zoomcamp Data loading workshop
# 
# Hello folks, let's practice what we learned - Loading data with the best practices of data engineering.
# 
# Here are the exercises we will do
# 
# 
# 
# # 1. Use a generator
# 
# Remember the concept of generator? Let's practice using them to futher our understanding of how they work.
# 
# Let's define a generator and then run it as practice.
# 
# **Answer the following questions:**
# 
# - **Question 1: What is the sum of the outputs of the generator for limit = 5?**
# - **Question 2: What is the 13th number yielded**
# 
# I suggest practicing these questions without GPT as the purpose is to further your learning.



def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1
# Example usage:
limit = 5
generator = square_root_generator(limit)

for sqrt_value in generator:
    print(sqrt_value)


# 

# # 2. Append a generator to a table with existing data
# 
# 
# Below you have 2 generators. You will be tasked to load them to duckdb and answer some questions from the data
# 
# 1. Load the first generator and calculate the sum of ages of all people. Make sure to only load it once.
# 2. Append the second generator to the same table as the first.
# 3. **After correctly appending the data, calculate the sum of all ages of people.**
# 
# 
# 



def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

for person in people_1():
    print(person)


def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


for person in people_2():
    print(person)


# 

# # 3. Merge a generator
# 
# Re-use the generators from Exercise 2.
# 
# A table's primary key needs to be created from the start, so load your data to a new table with primary key ID.
# 
# Load your first generator first, and then load the second one with merge. Since they have overlapping IDs, some of the records from the first load should be replaced by the ones from the second load.
# 
# After loading, you should have a total of 8 records, and ID 3 should have age 33.
# 
# Question: **Calculate the sum of ages of all the people loaded as described above.**
# 

# # Solution: 
## Question 1
limit = 5
generator = square_root_generator(limit)
answer1 = 0
for sqrt_value in generator:
    answer1 += sqrt_value

## Question 2
limit = 13
generator = square_root_generator(limit)
for sqrt_value in generator:
    answer2 = sqrt_value

## Question 3 

import dlt

# define the connection to load to. 
# We now use duckdb, but you can switch to Bigquery later
Q3pipeline = dlt.pipeline(pipeline_name="hw_data",
						destination='duckdb', 
						dataset_name='hw3')

# run the pipeline with default settings, and capture the outcome
# pass the generator (people_1) as  data
info = Q3pipeline.run(people_1(),
                      table_name="users",
					  write_disposition="replace")

info = Q3pipeline.run(people_2(),
                      table_name="users",
					  write_disposition="append")
# show the outcome
print(info)


import duckdb
conn = duckdb.connect(f"{Q3pipeline.pipeline_name}.duckdb")
conn.sql(f"SET search_path = '{Q3pipeline.dataset_name}'")


print('Loaded tables: ')
display(conn.sql("show tables"))

# and the data
print("\n\n\n users table below:")

users = conn.sql("SELECT * FROM users").df()
display(users)

answer3 = conn.sql("SELECT sum(age) as 'answer' FROM users").df()




## Question 4

import dlt

# define the connection to load to. 
# We now use duckdb, but you can switch to Bigquery later
Q4pipeline = dlt.pipeline(pipeline_name="hw_data",
						destination='duckdb', 
						dataset_name='hw3')

# run the pipeline with default settings, and capture the outcome
# pass the generator (people_1) as  data
info = Q4pipeline.run(people_1(),
                      table_name="users",
					  write_disposition="replace")

info = Q4pipeline.run(people_2(),
                      table_name="users",
					  write_disposition="merge",
                      primary_key = "id")
# show the outcome
print(info)

print('Loaded tables: ')
display(conn.sql("show tables"))

# and the data
print("\n\n\n users table below:")

users = conn.sql("SELECT * FROM users").df()
display(users)

answer4 = conn.sql("SELECT sum(age) as 'answer' FROM users").df()
#display(answer)

# Answers

print('Answer for Q1 :', answer1)
print('Answer for Q2 :', answer2)  
print('Answer for Q3 :', answer3['answer'].values[0])  
print('Answer for Q4 :', answer4['answer'].values[0])  




