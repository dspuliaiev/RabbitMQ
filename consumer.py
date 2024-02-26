import pika
import json
from models import Contact
from mongoengine import connect

def send_email(contact_id):
    print(f"Sending email to contact with ID: {contact_id}")

    contact = Contact.objects.get(id=contact_id)
    contact.message_sent = True
    contact.save()
    print("Email sent successfully!")

def callback(ch, method, properties, body):
    message = json.loads(body)
    contact_id = message['contact_id']
    send_email(contact_id)

def main():
    connect(db="hw", host="mongodb+srv://dspuliaiev:Pp4125NZf4h4JG22@cluster0.j1fuex0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='email_contacts')

    # Підписка на отримання повідомлень
    channel.basic_consume(queue='email_contacts', on_message_callback=callback, auto_ack=True)

    print('Consumer started. Waiting for messages...')
    channel.start_consuming()

if __name__ == "__main__":
    main()