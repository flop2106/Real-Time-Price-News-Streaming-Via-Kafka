# PROJECT OVERVIEW
# Objective:
This git repository is to demonstrate the usage of Apache Kafka in real-time to near-real-time streaming of data.

# Pipeline Architecture:
1) Kafka is set up with 3 broker, with 3 partitions and 3 replication factors.
2) 4 topics are created in kafka which are news.economy, price.economy, news.technology and price.technology.
3) stocks_publisher.py will start a headerless selenium, go to investing.com url and fetch the current ticker price repeatedly in 5 seconds. The price fetched are directly published to kafka.
4) news_publisher.py in the other hand call the NewsAPI query and directly publish the result to kafka.
6) There are no automatic trigger created for this project, user can fetch the data required by triggering either stocks_publisher.py and news_publisher.py.
5) html_creator.py will consume the data from Kafka and create 2 html to demonstrate the result: economy.html and technology.html

# INSTALLATION GUIDE:

# Install Docker In WSL:
sudo apt update
sudo apt install -y ca-certificates curl gnupg lsb-release

# Add Docker’s official GPG key
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Set up the stable repository
echo \
  "deb [arch=$(dpkg --print-architecture) \
  signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

INSTALL/STARTUP KAFKA USING CONFLUENT:
docker-compose up -d

CHECK KAFKA IS RUNNING
docker-compose ps

SHUTDOWN KAFKA:
docker-compose down -v

