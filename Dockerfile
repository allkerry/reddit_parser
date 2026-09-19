FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY scraper/ ./scraper/
COPY top_subreddits.csv .

# config.yaml, accounts.yaml и cookies/ монтируются как volume в docker-compose.yml,
# чтобы их можно было менять (в т.ч. включать новые аккаунты, крутить target_rate)
# без пересборки образа.

CMD ["python3", "main.py"]
