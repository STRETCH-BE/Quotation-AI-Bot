#!/usr/bin/env python3
"""
Insert Star Ceiling Products into STRETCH Bot Database
Run on your Azure VM: python3 insert_starceiling.py
"""
import pymysql

# Connect to database
conn = pymysql.connect(
    host='aibot.mysql.database.azure.com',
    port=3306,
    user='STRETCH',
    password='Plafondlux.99',
    database='chatbot_db',
    ssl={'ssl': True},
    cursorclass=pymysql.cursors.DictCursor
)

cursor = conn.cursor()

# Insert SQL - matches your actual table structure
sql = """
INSERT INTO products (
    product_code, 
    description, 
    base_category, 
    unit, 
    sort_order, 
    is_active, 
    price_b2c, 
    price_b2b_reseller, 
    price_b2b_hospitality
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE 
    description = VALUES(description), 
    price_b2c = VALUES(price_b2c), 
    price_b2b_reseller = VALUES(price_b2b_reseller), 
    price_b2b_hospitality = VALUES(price_b2b_hospitality),
    is_active = VALUES(is_active)
"""

# Your 4 star ceiling products (prices converted from EU format)
products = [
    (
        'S Plafond SC 25 - RGBW - Twinkle',
        'STRETCH Starceiling - 25 starts - RGBW + Twinkle',
        'light',
        'm2',
        100,
        1,
        152.41,  # price_b2c
        127.00,  # price_b2b_reseller
        135.70   # price_b2b_hospitality
    ),
    (
        'S Plafond SC 50 - RGBW - Twinkle',
        'STRETCH Starceiling - 50 starts - RGBW + Twinkle',
        'light',
        'm2',
        100,
        1,
        222.97,
        185.81,
        198.54
    ),
    (
        'S Plafond SC 75 - RGBW - Twinkle',
        'STRETCH Starceiling - 75 starts - RGBW + Twinkle',
        'light',
        'm2',
        100,
        1,
        296.54,
        247.12,
        264.05
    ),
    (
        'S Plafond SC 100 - RGBW - Twinkle',
        'STRETCH Starceiling - 100 starts - RGBW + Twinkle',
        'light',
        'm2',
        100,
        1,
        373.67,
        311.39,
        332.72
    ),
]

print("=" * 50)
print("🌟 Inserting Star Ceiling Products")
print("=" * 50)

for p in products:
    try:
        cursor.execute(sql, p)
        print(f"✅ {p[0]}")
    except Exception as e:
        print(f"❌ {p[0]}: {e}")

conn.commit()
print(f"\n✅ Inserted/Updated {len(products)} products!")

# Verify the insert
print("\n📋 Verification:")
print("-" * 50)
cursor.execute("""
    SELECT product_code, description, price_b2c, price_b2b_reseller, price_b2b_hospitality 
    FROM products 
    WHERE product_code LIKE 'S Plafond SC%'
    ORDER BY product_code
""")

for row in cursor.fetchall():
    print(f"  {row['product_code']}")
    print(f"    B2C: €{row['price_b2c']:.2f} | B2B Reseller: €{row['price_b2b_reseller']:.2f} | B2B Hospitality: €{row['price_b2b_hospitality']:.2f}")

cursor.close()
conn.close()

print("\n" + "=" * 50)
print("✅ Done!")
print("=" * 50)