from pika_multithreaded.clients import AsyncConnection

# Edit these lines to change the connection parameters
protocol = "amqps"
host = "b-b637fc85-8ade-43f7-b626-bbf2b40d8d98.mq.us-gov-west-1.amazonaws.com"
port = "5671"
queue = "test-queue"
user = "ryan-cli-test"
password = "QPwkB4Y5o4LB3jvW0TGE"
# DON'T EDIT THE FOLLOWING LINE
url = f"{protocol}://{user}:{password}@{host}:{port}"

print(">>> Creating connection object...")
my_connection = AsyncConnection(url=url)
print(">>> Connection object created!")
print(">>> Sending message...")
my_connection.send_message(
    routing_key="test-queue",
    message="this is a test message. can you hear me???")

print(">>> Message sent successfully!!! (maybe)")
