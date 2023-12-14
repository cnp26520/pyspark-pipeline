try:
    from kafka import KafkaProducer
    from faker import Faker
    import json
    from time import sleep
    import uuid
except Exception as e:
    pass

#Creating lists for Data to be chosen from
states = ('AK', 'AL', 'AR', 'AZ', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'MD','MN', 'MO', 'MT', 'NH', 'NJ', 'NM', 'NV', 'NY', 'RI', 'SC', 'TX', 'VA', 'VT', 'WA', 'WI', 'WV', '' )
fnames = ("Angel", "Angelo", "Angus", "Anir", "Anis", "Anish", "Anmolpreet", "Annan", "Anndra", "Anselm", "Anthony", "Corie", "Corin", "Cormac", "Cormack", "Cormak", "Corran", "Corrie", "Cory", "Cosmo", "Coupar", "Craig", "Leigham", "Leighton", "Leilan", "Leiten", "Leithen", "Leland", "Lenin", "Lennan", "Lennen", "Lennex", "Lennon", "Lennox", "Lenny", "Leno", "Lenon", "Lenyn", "Leo", "Leon", "Leonard", "Leonardas", "Leonardo", "Lepeng", "Leroy", "Leven", "Levi", "Levon", "Levy", "Lewie", "Lewin", "Lewis", "Lex", "Leydon", "Leyland", "Leylann", "Leyton", "Liall", "Liam", "Liam-Stephen", "Limo", "Lincoln", '')
lnames = ('Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker', 'Young', '')
small_ints = tuple(range(1, 101))
medium_ints = tuple(range(1000,10001))
large_ints = tuple(range(10000, 100001))

field_choices = []
header_choices = []
choice = 100

#while loop to hold while inputting data fields and sources they pull from
while choice != 0:
    choice = int(input("\n Pick a field for your data \n 0. Exit\n 1. States\n 2. First Names\n 3. Last Names\n 4. Small Numbers\n 5. Medium Numbers\n 6. Large Numbers\n"))
    if choice == 1:
        field_choices.append(states)
    elif choice == 2:
        field_choices.append(fnames)
    elif choice == 3:
        field_choices.append(lnames)
    elif choice == 4:
        field_choices.append(small_ints)
    elif choice == 5:
        field_choices.append(medium_ints)
    elif choice == 6:
        field_choices.append(large_ints)
    elif choice == 0:
        break
    else: print("Please enther the number of your desired option\n")

    header_name = input("Enter a header for your column\n")
    header_name.replace(' ', "_")
    header_choices.append(header_name)


sets_of_10 = int(input("How many sets of 10 records do you want to populate\n"))

#Initializes the kafka producer and faker 
producer = KafkaProducer(bootstrap_servers='localhost:9092')


global faker

faker = Faker()


#Creates a list of arguments and adds to it dynamically for data generators
def faking_it():
    args=[uuid.uuid4().__str__(),
                faker.random_element(elements=fnames),
                faker.random_element(elements=('IT', 'HR', 'Sales', 'Marketing', '')),
                faker.random_element(elements=states),
                faker.random_element(elements=small_ints)]
    for i in field_choices:
        args.append(faker.random_element(elements=i))
    return args

#Generates data for the producer to send to spark
class DataGenerator(object):

    @staticmethod
    def get_data():
        return faking_it()
                

#defines the column headers and then packages it with the generated data and sends it out
columns =  ["emp_id", "employee_name", "department", "state", "age"]
for i in header_choices:
    columns.append(i)
print(columns)

for _ in range(sets_of_10):

    for i in range(1,11):
        data_list = DataGenerator.get_data()
        json_data = dict(
            zip
            (columns,data_list

            )
        )
        _payload = json.dumps(json_data).encode("utf-8")
        response = producer.send('FirstTopic', _payload)
        print(_payload)
        sleep(1)

