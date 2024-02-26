import pika
import json
from faker import Faker
from mongoengine import connect
from models import Contact


connect(db="hw", host="mongodb+srv://dspuliaiev:Pp4125NZf4h4JG22@cluster0.j1fuex0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")


credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
channel = connection.channel()


def send_contacts():
    fake = Faker()
    for _ in range(10):
        fullname = fake.name()
        email = fake.email()
        contact = Contact(fullname=fullname, email=email)
        contact.save()

        message = {'contact_id': str(contact.id)}
        channel.basic_publish(exchange='', routing_key='email_contacts', body=json.dumps(message))
        print(f"Sent contact with ID: {contact.id}")

if __name__ == '__main__':
    send_contacts()
    connection.close()