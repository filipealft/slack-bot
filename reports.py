import mysql.connector
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import time 
import os
import mysql.connector.errors as mce

def connect_to_database():
    
    return mysql.connector.connect(
        host=os.environ['DB_HOST'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        database=os.environ['DB_DATABASE'],
        port=os.environ['DB_PORT']
    )

def send_daily_report(connection):
    
    cursor = connection.cursor()
    query = """
        select 
            main.trade_date
            , sum_til_now  
            , day_result
        from 
            (
            SELECT distinct 
                date(bet.operation_time) trade_date, 
                SUM(profit) OVER (ORDER BY date(bet.operation_time)) AS sum_til_now
            FROM 
                bet
            where 
                (bet_type is null or bet_type = 'real')
            order by trade_date desc
            ) as main
        join 
            (
            select distinct 
                date(bet.operation_time) trade_date, 
                SUM(profit) day_result
                , case when SUM(profit) > 0 then sum(profit) else null end day_result_win
                , case when SUM(profit) < 0 then sum(profit) else null end day_result_loss
            from 
                bet
            where 
                date(operation_time) = curdate() - interval 1 day
                and (bet_type is null or bet_type = 'real')
            group by date(operation_time)
            order by trade_date desc
            ) as second on second.trade_date = main.trade_date
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()

    message = "Olá pessoal!\n\nSegue abaixo o relatório de resultados de ontem:\n\n"
    for row in results:
        data = row[0].strftime("%d/%m/%Y")
        faturamento = row[1]
        resultado = row[2]
        if resultado > 0:
            status = "🟢 Positivo"
        elif resultado < 0:
            status = "🔴 Negativo"
        else:
            status = "🟡 Neutro"
        message += f"📅 Data: {data}\n💰 Faturamento bruto: {faturamento:.2f}\n💸 Resultado do dia: {resultado:.2f}\n{status}\n\n"

    client = WebClient(token=os.environ['SLACK_TOKEN'])
    try:
        client.chat_postMessage(channel="#report_test", text=message)
        print("Mensagem enviada com sucesso")
    except SlackApiError as e:
        print("Erro ao enviar a mensagem: {}".format(e))


def get_last_trade(connection):
    
    cursor = connection.cursor()
    cursor.execute("SELECT MAX(id) FROM bet")
    latest_id = cursor.fetchone()[0]
    client = WebClient(token=os.environ['SLACK_TOKEN'])
    
    while True:
        try:
            cursor.execute("SELECT MAX(id) FROM bet")
            current_id = cursor.fetchone()[0]

            if current_id != latest_id:
                query = """
                    SELECT
                        value,
                        profit,
                        pair,
                        operation_time,
                        result
                    FROM
                        bet
                    WHERE
                        (bet_type IS NULL OR bet_type = 'real')
                        AND result IS NOT NULL
                    ORDER BY
                        id DESC
                    LIMIT 1
                """
                cursor.execute(query)
                result = cursor.fetchone()

                value = result[0]
                profit = result[1]
                pair = result[2]
                operation_time = result[3].strftime("%d/%m/%Y %H:%M:%S")
                result = result[4]

                message = f"🚨 Novo trade! 🚨\n\n"
                message += f"💰 Valor: {value:.2f}\n"
                message += f"💸 Lucro/Prejuízo: {profit:.2f}\n"
                message += f"🔍 Paridade: {pair}\n"
                message += f"⏰ Data/Hora: {operation_time}\n"
                message += f"🏦 Resultado: {result}\n"

                try:
                    client.chat_postMessage(channel="#report_test", text=message)
                    print("Mensagem enviada com sucesso")
                except SlackApiError as e:
                    print("Erro ao enviar a mensagem: {}".format(e))

                latest_id = current_id

            connection.commit()
            time.sleep(60)
            
        except mce.OperationalError as e:
            print(f"Erro de conexão: {e}")
            print("Tentando reconectar...")
            connection = connect_to_database()
            cursor = connection.cursor()
            continue
