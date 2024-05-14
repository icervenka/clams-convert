FROM python:3.12 AS base
WORKDIR /usr/local/app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy 
COPY . .


