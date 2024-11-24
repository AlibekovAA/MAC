import pika
from pika.spec import Basic
import time
import json
from typing import Dict
import threading
import psutil
import smtplib
from email.mime.text import MIMEText
import logging
import os


logging.basicConfig(
    filename='MAC.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

logging.getLogger('pika').setLevel(logging.WARNING)


class RabbitMQConnection:
    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.connect()

    def connect(self) -> None:
        while True:
            try:
                rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'localhost')
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=rabbitmq_host, heartbeat=180)
                )
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=self.queue_name)
                print(f"Connected to RabbitMQ: {self.queue_name}")
                break
            except pika.exceptions.AMQPConnectionError as e:
                print(f"Connection failed, retrying in 5 seconds... Error: {e}")
                time.sleep(5)

    def close(self) -> None:
        if self.connection:
            self.connection.close()


class MetricCollector(RabbitMQConnection):
    def collect_metrics(self) -> Dict[str, float]:
        return {
            "cpu_usage": psutil.cpu_percent(interval=1),
            "memory_usage": psutil.virtual_memory().percent,
            "disk_io": psutil.disk_io_counters().read_bytes + psutil.disk_io_counters().write_bytes,
            "disk_usage": psutil.disk_usage('/').percent,
            "process_count": len(psutil.pids()),
            "network_io_sent": psutil.net_io_counters().bytes_sent,
            "network_io_recv": psutil.net_io_counters().bytes_recv,
        }

    def send_metrics(self) -> None:
        try:
            metrics = self.collect_metrics()
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=json.dumps(metrics))
            logging.info(f"Sent metrics: {metrics}")
        except pika.exceptions.AMQPConnectionError:
            logging.warning("Lost connection to RabbitMQ, reconnecting...")
            self.connect()


class MetricAnalyzer(RabbitMQConnection):
    def __init__(self, queue_name: str, notification_queue: str):
        super().__init__(queue_name)
        self.notification_queue = notification_queue
        self.alert_states = {
            "high_cpu": False,
            "high_memory": False,
            "high_disk_io": False,
            "high_disk_usage": False,
            "high_process_count": False,
            "high_network_traffic": False,
            "high_uptime": False
        }

    def analyze_metrics(self, ch: pika.adapters.blocking_connection.BlockingChannel,
                        method: Basic.Deliver, properties: pika.BasicProperties, body: bytes) -> None:
        metrics = json.loads(body)
        logging.info(f"Received metrics: {metrics}")
        self.detect_issues(metrics)

    def detect_issues(self, metrics: Dict[str, float]) -> None:
        if metrics['cpu_usage'] > 80 and not self.alert_states["high_cpu"]:
            self.send_notification("High CPU Usage!")
            self.alert_states["high_cpu"] = True
        elif metrics['cpu_usage'] <= 80 and self.alert_states["high_cpu"]:
            self.alert_states["high_cpu"] = False

        if metrics['memory_usage'] > 90 and not self.alert_states["high_memory"]:
            self.send_notification("High Memory Usage!")
            self.alert_states["high_memory"] = True
        elif metrics['memory_usage'] <= 90 and self.alert_states["high_memory"]:
            self.alert_states["high_memory"] = False

        if metrics['disk_io'] > 1e9 and not self.alert_states["high_disk_io"]:
            self.send_notification("High Disk I/O Usage!")
            self.alert_states["high_disk_io"] = True
        elif metrics['disk_io'] <= 1e9 and self.alert_states["high_disk_io"]:
            self.alert_states["high_disk_io"] = False

        if metrics['disk_usage'] > 90 and not self.alert_states["high_disk_usage"]:
            self.send_notification("Disk Usage is Critically High!")
            self.alert_states["high_disk_usage"] = True
        elif metrics['disk_usage'] <= 90 and self.alert_states["high_disk_usage"]:
            self.alert_states["high_disk_usage"] = False

        if metrics['process_count'] > 300 and not self.alert_states["high_process_count"]:
            self.send_notification("High Number of Active Processes!")
            self.alert_states["high_process_count"] = True
        elif metrics['process_count'] <= 300 and self.alert_states["high_process_count"]:
            self.alert_states["high_process_count"] = False

        if (metrics['network_io_sent'] > 1e8 or metrics['network_io_recv'] > 1e8) and not self.alert_states["high_network_traffic"]:
            self.send_notification("High Network Traffic Detected!")
            self.alert_states["high_network_traffic"] = True
        elif metrics['network_io_sent'] <= 1e8 and metrics['network_io_recv'] <= 1e8 and self.alert_states["high_network_traffic"]:
            self.alert_states["high_network_traffic"] = False

        if not any(self.alert_states.values()):
            self.send_notification("All metrics are within normal ranges. System is OK!")

    def send_notification(self, message: str) -> None:
        self.channel.basic_publish(exchange='', routing_key=self.notification_queue, body=json.dumps({"message": message}))

    def start_consuming(self) -> None:
        while True:
            try:
                self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.analyze_metrics, auto_ack=True)
                logging.info("Waiting for metrics...")
                self.channel.start_consuming()
            except pika.exceptions.AMQPConnectionError as e:
                logging.warning(f"Lost connection to RabbitMQ, reconnecting... Error: {e}")
                self.connect()
            except Exception as e:
                logging.error(f"Error while consuming: {e}")
                break


class NotificationAgent(RabbitMQConnection):
    def __init__(self, queue_name: str, email: str, password: str):
        super().__init__(queue_name)
        self.email = email
        self.password = password

    def send_email(self, subject: str, message: str) -> None:
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = self.email
        msg['To'] = self.email

        try:
            with smtplib.SMTP('smtp.mail.ru', 587) as server:
                server.starttls()
                server.login(self.email, self.password)
                server.send_message(msg)
        except Exception as e:
            logging.error(f"Failed to send email: {e}")

    def send_notification(self, message: str) -> None:
        self.send_email("Alert Notification", message)
        logging.info(f"ALERT: {message}")

    def start_consuming(self) -> None:
        try:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.handle_notification, auto_ack=True)
            logging.info("Waiting for notifications...")
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            logging.warning("Lost connection to RabbitMQ, reconnecting...")
            self.connect()

    def handle_notification(self, ch: pika.adapters.blocking_connection.BlockingChannel,
                            method: Basic.Deliver, properties: pika.BasicProperties, body: bytes) -> None:
        notification = json.loads(body)
        self.send_notification(notification["message"])


def run_analyzer(queue_name: str, notification_queue: str) -> None:
    analyzer = MetricAnalyzer(queue_name, notification_queue)
    analyzer.start_consuming()


def run_notification_agent(queue_name: str, email: str, password: str) -> None:
    notification_agent = NotificationAgent(queue_name, email, password)
    notification_agent.start_consuming()


def main() -> None:
    collector = MetricCollector(queue_name='metrics')

    analyzer_thread = threading.Thread(target=run_analyzer, args=('metrics', 'notifications'))
    analyzer_thread.start()

    email = "email"
    password = "password"
    notification_agent_thread = threading.Thread(target=run_notification_agent, args=('notifications', email, password))
    notification_agent_thread.start()

    try:
        while True:
            collector.send_metrics()
            time.sleep(300)
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        collector.close()
        analyzer_thread.join()
        notification_agent_thread.join()


if __name__ == "__main__":
    main()
