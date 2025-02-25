FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy all project files
COPY . /app

# Set up environment and install dependencies
RUN pip install --no-cache-dir -r /app/myanimelist/requirements.txt

EXPOSE 8080
# Set the default command to run the scraper
# CMD ["/bin/sh", "-c", "pwd && ls -la && ls -la myanimelist/src/mal_anime && sleep 3600"]

CMD ["uvicorn", "myanimelist.src.mal_anime.app:app", "--host", "0.0.0.0", "--port", "8080"]