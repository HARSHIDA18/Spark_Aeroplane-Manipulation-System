import time
from datetime import datetime
import random
from faker import Faker
import json

fake = Faker()
airline_names = ["Indigo", "AirIndia", "Quatar", "SingaporeAirlines"]
destination_city = ["India", "Amsterdam", "Norway", "Singapore"]
source_city = ["India", "Amsterdam", "Norway", "Singapore"]

def generate_first_file_data():
    data = {
        "aircraft_id": random.randint(1, 1000),
        "airlines_name": random.choice([random.choice(airline_names), None] * 1),
        "travel": {
            "destination_city": random.choice(destination_city),
            "source_city": random.choice(destination_city)
        },
        "aircraftEmergency_phone": fake.phone_number(),
        "time": {
            "departure_time": fake.time(pattern="%H:%M:%S"),
            "arrival_time": fake.time(pattern="%H:%M:%S")
        }
    }
    return data

def generate_second_file_data(common_aircraft_id):
    data = {
        "aircraft_id": random.randint(1, 1000),
        "aircraft_rating": float(random.choice([round(random.uniform(1, 10), 2) if i != 0 else 0 for i in range(11)])),
        "price": {
            "INR": random.choice([round(random.uniform(5000, 20000), 2)]),
            "USD": random.choice([round(random.uniform(100, 9000), 2)])
        }
    }
    return data

if __name__ == "__main__":
    try:
        curr_time = datetime.now()

        # Number of entries to generate
        num_entries_first_file = 30
        num_entries_second_file = 40

        common_aircraft_id = random.randint(1, 1000)

        with open("FirstAeroplaneJSON.json", "w") as json_file:
            for _ in range(num_entries_first_file):
                generated_data = generate_first_file_data()
                json_data = json.dumps(generated_data)
                json_file.write(json_data + ',\n')
                time.sleep(1)

        with open("SecondAeroplaneJSON.json", "w") as json_file:
            for _ in range(num_entries_second_file):
                generated_data = generate_second_file_data(common_aircraft_id)
                json_data = json.dumps(generated_data)
                json_file.write(json_data + ',\n')
                time.sleep(1)

        print(f"{num_entries_first_file} fake data entries have been generated and appended to FirstAeroplaneJSON.json.")
        print(f"{num_entries_second_file} fake data entries have been generated and appended to SecondAeroplaneJSON.json.")
    except Exception as e:
        print(f"An error occurred: {e}")
