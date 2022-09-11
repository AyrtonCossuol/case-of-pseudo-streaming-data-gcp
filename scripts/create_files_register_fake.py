from typing import Any
from faker import Faker
from datetime import datetime
import random as r  
import os

def generate_register_fake(file, fake, qt_register_fake: int):
    """
    Funcao que ira gerar os registros de forma aleatoria e inserir dentro do arquivo csv
    :file: arquivo que sera inseridos os registros fakes criados.
    :fake: instancia do pacote Fake
    :qt_register_fake: quantidade aleatoria de registros a serem criados e inseridos no arquivo csv

    return
        Arquivo com a quantidade de registros inseridos de acordo com o valor de qt_register_fake.
     """

    file.write('username, name, sex,address, email, birthdate, date\n')
    for i in range(0, qt_register_fake):
        simple_profile = fake.simple_profile()
        user_fake       = simple_profile['username']
        name_fake       = simple_profile['name']
        sex_fake        = simple_profile['sex']
        address_fake    = ' '.join(simple_profile['address'].split('\n'))
        email_fake      = simple_profile['mail']
        birthdate_fake  = simple_profile['birthdate']
        datetime_injest = f'{datetime.now().strftime("%Y%m%d%H%M%S")}'
        
        file.write(f'{user_fake}, {name_fake}, {sex_fake}, {address_fake}, {email_fake}, {birthdate_fake}, {datetime_injest}\n')
    
    return file

def create_file_fake() -> None:
    """
    Funcao que ira gerar uma quantidade N de arquivos com registros fakes.
    """
    fake = Faker('pt-BR')
    N = 4
    
    for _ in range(0, N):
        name_file_fake = f'./datasets-fakes/register_people_fake_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
        qt_register_fake = r.randint(6000, 10001)
        file = open(name_file_fake, 'w', encoding='utf-8')
        
        file = generate_register_fake(file, fake, qt_register_fake)

        file.close()
        print("Quantidade de registros gerados = ", qt_register_fake)
    
    return None

if __name__ == '__main__':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\ayrto\OneDrive\Área de Trabalho\students\project-student-account.json"
    create_file_fake()