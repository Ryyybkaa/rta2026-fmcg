from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

MARKI   = ['Tyskie', 'Lech', 'Zubr', 'Debowe Mocne', 'Ksiazece']
REGIONY = ['Wielkopolska', 'Mazowsze', 'Slask', 'Malopolska', 'Pomorze']
KANALY  = ['hipermarket', 'supermarket', 'convenience', 'gastronomia']
CENY    = {'Tyskie': 3.49, 'Lech': 3.59, 'Zubr': 2.99, 'Debowe Mocne': 3.19, 'Ksiazece': 4.29}

for i in range(1000):
    marka = random.choice(MARKI)
    cena_bazowa = CENY[marka]

    # 5% anomalii
    if random.random() < 0.05:
        ilosc = random.randint(50, 100)
        cena  = round(cena_bazowa * random.uniform(0.4, 0.6), 2)
    else:
        ilosc = random.randint(1, 12)
        cena  = round(cena_bazowa * random.uniform(0.95, 1.05), 2)

    tx = {
        'tx_id':     f'TX{i+1:05d}',
        'timestamp': datetime.now().isoformat(),
        'region':    random.choice(REGIONY),
        'kanal':     random.choice(KANALY),
        'marka':     marka,
        'ilosc':     ilosc,
        'cena_jedn': cena,
        'wartosc':   round(ilosc * cena, 2),
        'promocja':  random.random() < 0.2
    }
    producer.send('sprzedaz', value=tx)
    print(f"[{i+1}] {tx['marka']:15s} | {tx['region']:15s} | {tx['ilosc']}szt | {tx['wartosc']:.2f} PLN")
    time.sleep(0.3)

producer.flush()
producer.close()
