from send_email import send_mail
from get_data import get_bets_data
from pretty_html_table import build_table


def send_bets_email():
    data = get_bets_data()
    output = build_table(data, 'blue_light')
    send_mail(output)

    print('Mail sent successfully!')
    return 'Mail sent successfully!'


if __name__ == "__main__":
    send_bets_email()
