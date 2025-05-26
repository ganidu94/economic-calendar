import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pytz
import schedule
import time
from discord_webhook import DiscordWebhook
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# List of 2025 US bank holidays (month, day)
BANK_HOLIDAYS_2025 = [
    (1, 1),   # New Year's Day
    (1, 20),  # Martin Luther King Jr. Day
    (2, 17),  # Washington's Birthday
    (5, 26),  # Memorial Day
    (6, 19),  # Juneteenth
    (7, 4),   # Independence Day
    (9, 1),   # Labor Day
    (10, 13), # Columbus Day/Indigenous Peoples' Day
    (11, 11), # Veterans Day
    (11, 27), # Thanksgiving Day
    (12, 25)  # Christmas Day
]

def is_bank_holiday(date):
    """Check if the given date is a US bank holiday in 2025."""
    return (date.month, date.day) in BANK_HOLIDAYS_2025

def get_us_high_impact_events(target_date):
    """Scrape high-impact US economic events from Forex Factory for the given date (EST)."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    url = 'https://www.forexfactory.com/calendar'
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch Forex Factory calendar: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    est = pytz.timezone('US/Eastern')
    events = []

    table = soup.find('table', class_='calendar__table')
    if not table:
        logging.warning("No calendar table found on Forex Factory page.")
        return events

    rows = table.find_all('tr', class_='calendar__row--event')
    for row in rows:
        date_cell = row.find('td', class_='calendar__cell--date')
        if not date_cell:
            continue
        date_text = date_cell.get_text(strip=True)
        try:
            event_date = datetime.strptime(date_text, '%b %d, %Y').date()
        except ValueError:
            event_date = target_date

        if event_date != target_date:
            continue

        currency_cell = row.find('td', class_='calendar__cell--currency')
        if currency_cell and currency_cell.get_text(strip=True) != 'USD':
            continue

        impact_cell = row.find('td', class_='calendar__cell--impact')
        if not impact_cell or 'impact--high' not in impact_cell.get('class', []):
            continue

        time_cell = row.find('td', class_='calendar__cell--time')
        event_cell = row.find('td', class_='calendar__cell--event')
        time = time_cell.get_text(strip=True) if time_cell else 'N/A'
        event_name = event_cell.get_text(strip=True) if event_cell else 'Unknown'

        events.append({'time': time, 'event': event_name})

    logging.info(f"Fetched {len(events)} high-impact US events for {target_date}.")
    return events

def send_discord_notification(message, webhook_url):
    """Send a Discord notification with the given message."""
    try:
        webhook = DiscordWebhook(url=webhook_url, content=message)
        response = webhook.execute()
        if response.status_code == 204 or response.status_code == 200:
            logging.info("Discord notification sent successfully via discord-webhook.")
        else:
            logging.error(f"Failed to send Discord notification via discord-webhook: Status {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Error sending Discord notification via discord-webhook: {e}")
        # Fallback to direct requests
        try:
            payload = {'content': message}
            response = requests.post(webhook_url, json=payload, timeout=10)
            if response.status_code == 204 or response.status_code == 200:
                logging.info("Discord notification sent successfully via direct requests.")
            else:
                logging.error(f"Failed to send Discord notification via direct requests: Status {response.status_code}, Response: {response.text}")
        except Exception as e:
            logging.error(f"Error sending Discord notification via direct requests: {e}")

def job():
    """Run the job: check for bank holiday in IST and send appropriate Discord notification."""
    try:
        ist = pytz.timezone('Asia/Kolkata')
        today_ist = datetime.now(ist).date()
        est = pytz.timezone('US/Eastern')
        today_est = datetime.now(est).date()
        webhook_url = os.environ.get('DISCORD_WEBHOOK_URL', 'https://discord.com/api/webhooks/1376619768483807270/7jXVBgJq9VzjLRrK2jmIKNAgEitDZSzcAr12JFuGGjy75HmhpiI0RLNrbYLft3v17v_4')

        # Use IST date for bank holiday check
        if is_bank_holiday(today_ist):
            message = "It’s a bank holiday today."
            logging.info(f"Bank holiday detected on {today_ist} IST. Sending notification.")
        else:
            # Fetch events for the current IST date, converted to EST
            target_date = today_ist
            events = get_us_high_impact_events(target_date)
            if not events:
                message = f"No high-impact US economic events for {target_date.strftime('%b %d, %Y')}."
            else:
                message = f"**High-Impact US Economic Events for {target_date.strftime('%b %d, %Y')}:**\n"
                for event in events:
                    message += f"- {event['time']} EST: {event['event']}\n"
        
        send_discord_notification(message, webhook_url)
    except Exception as e:
        logging.error(f"Job failed: {e}")

# Set timezone to IST (+0530)
ist = pytz.timezone('Asia/Kolkata')
schedule.every().day.at("00:55", tz=ist).do(job)  # 12:00 AM IST

# Run the scheduler
while True:
    schedule.run_pending()
    time.sleep(60)