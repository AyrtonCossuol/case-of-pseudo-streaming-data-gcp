from google.cloud.pubsub import PublisherClient
from faker import Faker
from datetime import datetime
import time
import json
import os

def create_struct_mensager(user_fake: str, name_fake: str, sex_fake: str, address_fake: str, email_fake: str, birthdate_fake: str, datetime_injest: str) -> bytes:
    """
    Funcao que vai criar a estrutura JSON para ser enviada ao Pub/Sub.
    
    :user_fake: Username (String)
    :name_fake: Nome e Sobrenome (String)
    :sex_fake: Sexo (M - Masculino / F - Feminino) (String)
    :address_fake: Endereço completo (String)
    :email_fake: E-mail (String)
    :birthdate_fake: Data de nascimento (String)
    :datetime_injest: Data da injestao do registro para controle (String)

    return 
        Uma variavel do tipo JSON com os registros organizados
    """
    record = {
        "username": user_fake,
        "name": name_fake,
        'sex' : sex_fake,
        'address' : address_fake,
        'email' : email_fake,
        'birthdate' : birthdate_fake,
        'date' : datetime_injest
    }

    data_str = json.dumps(record, default = str)
    data = data_str.encode("utf-8")

    return data

def generate_register():
    """
    Funcao que ira gerar registros Fakes para a injestao no Pub/Sub, simulando cadastros de 
    pessoas em tempo real
    """

    fake = Faker('pt-BR')
    publisher_client = PublisherClient()

    topic_path = r'projects/project-student-361518/topics/reception-register-streaming'
    
    for i in range(0, 10):
        simple_profile = fake.simple_profile()
        user_fake       = simple_profile['username']
        name_fake       = simple_profile['name']
        sex_fake        = simple_profile['sex']
        address_fake    = simple_profile['address']
        email_fake      = simple_profile['mail']
        birthdate_fake  = simple_profile['birthdate']
        datetime_injest = f'{datetime.now().strftime("%Y%m%d%H%M%S")}'

        
        data = create_struct_mensager(user_fake, name_fake, sex_fake, address_fake, email_fake, birthdate_fake, datetime_injest)
        future = publisher_client.publish(topic_path, data)
        print(f"Published message ID: {future.result()}")
        time.sleep(2)

    return None

if __name__ == '__main__':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\ayrto\OneDrive\Área de Trabalho\students\project-student-account.json"
    generate_register()