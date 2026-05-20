from kafka import KafkaConsumer, KafkaProducer
import json

CENY_BAZOWE = {'Tyskie': 3.49, 'Lech': 3.59, 'Zubr': 2.99, 'Debowe Mocne': 3.19, 'Ksiazece': 4.29}

consumer = KafkaConsumer(
    'sprzedaz',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    group_id='fmcg-monitoring',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Konsument uruchomiony — monitoruje sprzedaz...")
print("-" * 70)

for message in consumer:
    tx = message.value
    cena_baz = CENY_BAZOWE.get(tx['marka'], tx['cena_jedn'])
    anomalia = False
    powod = []

    if tx['cena_jedn'] < cena_baz * 0.7:
        anomalia = True
        powod.append(f"niska cena {tx['cena_jedn']:.2f} vs bazowa {cena_baz:.2f}")

    if tx['ilosc'] > 30:
        anomalia = True
        powod.append(f"wysoki wolumen {tx['ilosc']} szt")

    if anomalia:
        alert_producer.send('alerty', value=tx)
        print(f"ALERT | {tx['marka']:15s} | {tx['region']:15s} | {', '.join(powod)}")
    else:
        print(f"OK    | {tx['marka']:15s} | {tx['region']:15s} | {tx['ilosc']}szt | {tx['cena_jedn']:.2f} PLN")
