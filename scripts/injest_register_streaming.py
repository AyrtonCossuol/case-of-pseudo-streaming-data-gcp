from google.cloud import pubsub_v1
from google.cloud.pubsub import PublisherClient
from faker import Faker
import json
import os

def generate_register():
    """
    Funcao que ira gerar registros Fakes para a injestao no Pub/Sub, simulando cadastros de 
    pessoas em tempo real
    """

    fake = Faker('pt-BR')
    publisher = pubsub_v1.PublisherClient()
    publisher_client = PublisherClient()

    topic_path = 'projects/project-student-361518/topics/reception-register-streaming'
    simple_profile = fake.simple_profile()

    

    user_fake       = simple_profile['username']
    name_fake       = simple_profile['name']
    sex_fake        = simple_profile['sex']
    address_fake    = simple_profile['address']
    email_fake      = simple_profile['mail']
    birthdate_fake  = simple_profile['birthdate']

    record = {
        "username": user_fake,
        "name": name_fake,
        'sex' : sex_fake,
        'address' : address_fake,
        'email' : email_fake,
        'birthdate' : birthdate_fake,
    }

    data_str = json.dumps(record, default=str)
    data = data_str.encode("utf-8")
    
    future = publisher_client.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")

    return None

if __name__ == '__main__':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\ayrto\OneDrive\Área de Trabalho\students\project-student-account.json"
    generate_register()