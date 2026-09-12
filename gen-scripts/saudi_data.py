"""
Saudi-localized reference data for the food delivery seeder.

All data here is real or realistic for Saudi Arabia:
- Cities: 5 largest cities by population
- Zones: real district names from each city (transliterated)
- Restaurant chains: real Saudi + international chains operating in KSA
- Cuisine types: reflects what's actually popular in KSA
- Names: transliterated Arabic names (first + tribal/family surnames)
- Review comments: mix of Arabic-transliterated and English, positive/neutral/negative
"""

# 5 largest Saudi cities with timezones
CITIES = [
    {"city_name": "Riyadh", "country_code": "SA", "timezone": "Asia/Riyadh"},
    {"city_name": "Jeddah", "country_code": "SA", "timezone": "Asia/Riyadh"},
    {"city_name": "Mecca", "country_code": "SA", "timezone": "Asia/Riyadh"},
    {"city_name": "Medina", "country_code": "SA", "timezone": "Asia/Riyadh"},
    {"city_name": "Dammam", "country_code": "SA", "timezone": "Asia/Riyadh"},
]

# Real district names per city (transliterated from Arabic)
ZONES_BY_CITY = {
    "Riyadh": [
        "Al Olaya", "Al Malaz", "Al Murabba", "Al Sulimaniyyah", "Al Rawdah",
        "Al Wurud", "Al Nakheel", "Al Yasmin", "Al Aqiq", "Al Ghadir",
        "Al Sahafah", "Al Izdihar", "Al Naseem", "Al Hamra", "Al Munsiyah",
        "Al Yarmouk", "Diplomatic Quarter", "Al Bujairi", "Al Wadi", "Al Khaleej",
    ],
    "Jeddah": [
        "Al Hamra", "Al Andalus", "Al Rawdah", "Al Salamah", "Al Naeem",
        "Al Shati", "Al Zahra", "Al Faisaliyyah", "Al Marwah", "Obhur",
        "Al Basateen", "Al Aziziyah", "Al Safa", "Al Mohammadiyyah", "Al Worood",
    ],
    "Mecca": [
        "Al Aziziyah", "Al Awali", "Al Naseem", "Al Shawqiyyah", "Al Zahir",
        "Ajyad", "Al Misfalah", "Al Hindawiyyah", "Al Jamiah",
    ],
    "Medina": [
        "Al Haram", "Quba", "Al Aziziyah", "Al Khalidiyah", "Al Aqool",
        "Bani Harithah", "Al Iskan", "Sayyid Al Shuhada",
    ],
    "Dammam": [
        "Al Faisaliyah", "Al Adamah", "Al Jalawiyah", "Al Manar", "Al Shati",
        "Al Anwar", "Al Noor", "Al Athir", "Al Mazruiyyah",
    ],
}

# Cuisine types popular in Saudi Arabia
CUISINE_TYPES = [
    "saudi", "lebanese", "indian", "american", "italian",
    "asian", "mexican", "yemeni", "turkish", "egyptian",
    "japanese", "burgers", "pizza", "shawarma", "seafood",
]

# Real restaurant chains operating in Saudi Arabia + plausible local names.
# Keeping these realistic so dim_restaurant has authentic-feeling values.
REAL_CHAINS = [
    ("Albaik", "saudi"),
    ("Kudu", "burgers"),
    ("Herfy", "burgers"),
    ("McDonald's", "american"),
    ("KFC", "american"),
    ("Burger King", "burgers"),
    ("Subway", "american"),
    ("Domino's Pizza", "pizza"),
    ("Pizza Hut", "pizza"),
    ("Hardee's", "burgers"),
    ("Texas Chicken", "american"),
    ("Shawarmer", "shawarma"),
    ("Al Tazaj", "saudi"),
    ("Al Romansiah", "saudi"),
    ("Najd Village", "saudi"),
    ("Bayt Al Mandi", "yemeni"),
    ("Al Mukalla", "yemeni"),
    ("Al Nakhla", "yemeni"),
    ("Maestro Pizza", "pizza"),
    ("Papa John's", "pizza"),
    ("Buffalo Wild Wings", "american"),
    ("Texas Roadhouse", "american"),
    ("Chili's", "american"),
    ("Applebee's", "american"),
    ("The Cheesecake Factory", "american"),
    ("Steak House", "american"),
    ("Yildizlar", "turkish"),
    ("Antalya Turkish Restaurant", "turkish"),
    ("Beirut Express", "lebanese"),
    ("Operation Falafel", "lebanese"),
    ("Zaatar w Zeit", "lebanese"),
    ("Barbar", "lebanese"),
    ("Bukhari Palace", "saudi"),
    ("Mathloutha House", "saudi"),
    ("Maharaja", "indian"),
    ("Bombay Chowpatty", "indian"),
    ("Tikka", "indian"),
    ("Wok Box", "asian"),
    ("Sumo Sushi", "japanese"),
    ("Toki", "japanese"),
    ("Yokari", "japanese"),
    ("Sushi Yoshi", "japanese"),
    ("Pasta Mania", "italian"),
    ("Italiano", "italian"),
    ("Romano's", "italian"),
    ("Taco Bell", "mexican"),
    ("Chipotle", "mexican"),
    ("Section B Cafe", "american"),
    ("Half Million", "american"),
    ("Dose Cafe", "american"),
    ("Caribou Coffee", "american"),
    ("Starbucks", "american"),
    ("Tim Hortons", "american"),
    ("Krispy Kreme", "american"),
    ("Cinnabon", "american"),
    ("Al Baba", "egyptian"),
    ("Koshary Abu Tarek", "egyptian"),
    ("Fish Market", "seafood"),
    ("Captain's Catch", "seafood"),
    ("Ocean Basket", "seafood"),
]

# Menu items with category and price range (in SAR — Saudi Riyals).
# Organized by cuisine so we can plausibly assign menus to restaurants.
MENU_ITEMS_BY_CUISINE = {
    "saudi": [
        ("Kabsa with Chicken", "main", 35, 55),
        ("Kabsa with Lamb", "main", 55, 85),
        ("Mandi Chicken", "main", 30, 50),
        ("Mandi Lamb", "main", 60, 90),
        ("Mathloutha", "main", 40, 60),
        ("Jareesh", "main", 25, 40),
        ("Saleeg", "main", 30, 45),
        ("Margoog", "main", 28, 42),
        ("Mutabbaq", "starter", 12, 22),
        ("Sambousa (4 pcs)", "starter", 8, 15),
        ("Hummus", "starter", 10, 18),
        ("Arabic Salad", "starter", 12, 20),
        ("Laban", "drink", 5, 10),
        ("Karak Chai", "drink", 5, 8),
        ("Saudi Coffee", "drink", 8, 15),
    ],
    "yemeni": [
        ("Mandi Chicken", "main", 30, 50),
        ("Mandi Lamb Half", "main", 80, 120),
        ("Mandi Lamb Full", "main", 150, 220),
        ("Madghout", "main", 40, 65),
        ("Haneeth", "main", 60, 95),
        ("Aseeda", "main", 30, 45),
        ("Yemeni Bread", "side", 5, 12),
        ("Sahawiq", "side", 6, 10),
        ("Yemeni Tea", "drink", 5, 9),
    ],
    "lebanese": [
        ("Shawarma Chicken", "main", 18, 28),
        ("Shawarma Beef", "main", 22, 32),
        ("Mixed Grill", "main", 65, 95),
        ("Kafta Mashawi", "main", 45, 65),
        ("Shish Tawook", "main", 40, 60),
        ("Hummus with Meat", "starter", 22, 32),
        ("Tabbouleh", "starter", 15, 25),
        ("Fattoush", "starter", 15, 25),
        ("Falafel (6 pcs)", "starter", 12, 20),
        ("Manakish Cheese", "starter", 12, 20),
        ("Manakish Zaatar", "starter", 10, 18),
        ("Jallab", "drink", 8, 15),
    ],
    "burgers": [
        ("Classic Burger", "main", 22, 35),
        ("Cheese Burger", "main", 25, 38),
        ("Double Cheese Burger", "main", 32, 48),
        ("Chicken Burger", "main", 22, 35),
        ("Spicy Chicken Burger", "main", 24, 38),
        ("Mushroom Burger", "main", 30, 45),
        ("French Fries", "side", 10, 18),
        ("Curly Fries", "side", 12, 20),
        ("Onion Rings", "side", 12, 20),
        ("Coleslaw", "side", 8, 14),
        ("Soft Drink", "drink", 5, 10),
        ("Milkshake", "drink", 12, 22),
    ],
    "pizza": [
        ("Margherita Pizza", "main", 28, 45),
        ("Pepperoni Pizza", "main", 35, 55),
        ("Chicken Ranch Pizza", "main", 38, 58),
        ("BBQ Chicken Pizza", "main", 38, 58),
        ("Vegetarian Pizza", "main", 32, 50),
        ("Four Cheese Pizza", "main", 40, 60),
        ("Garlic Bread", "side", 12, 22),
        ("Mozzarella Sticks", "side", 18, 28),
        ("Caesar Salad", "side", 18, 28),
        ("Soft Drink", "drink", 5, 10),
    ],
    "american": [
        ("Buffalo Wings (8 pcs)", "main", 30, 48),
        ("Chicken Tenders", "main", 25, 40),
        ("Crispy Chicken Sandwich", "main", 22, 35),
        ("Beef Steak", "main", 75, 120),
        ("Grilled Salmon", "main", 65, 95),
        ("Caesar Salad", "starter", 25, 38),
        ("Loaded Nachos", "starter", 28, 42),
        ("Mac and Cheese", "side", 18, 28),
        ("Baked Potato", "side", 12, 20),
        ("Iced Coffee", "drink", 12, 22),
        ("Fresh Juice", "drink", 12, 22),
    ],
    "shawarma": [
        ("Chicken Shawarma Wrap", "main", 12, 22),
        ("Beef Shawarma Wrap", "main", 15, 25),
        ("Chicken Shawarma Plate", "main", 22, 35),
        ("Beef Shawarma Plate", "main", 28, 42),
        ("Mixed Shawarma Plate", "main", 32, 48),
        ("Garlic Sauce", "side", 3, 6),
        ("Pickles Plate", "side", 5, 10),
        ("Pepsi", "drink", 5, 8),
    ],
    "indian": [
        ("Butter Chicken", "main", 35, 55),
        ("Chicken Tikka Masala", "main", 38, 58),
        ("Lamb Biryani", "main", 45, 70),
        ("Chicken Biryani", "main", 38, 58),
        ("Palak Paneer", "main", 30, 48),
        ("Naan Bread", "side", 6, 12),
        ("Garlic Naan", "side", 8, 14),
        ("Samosa (3 pcs)", "starter", 12, 20),
        ("Mango Lassi", "drink", 12, 20),
        ("Masala Chai", "drink", 8, 14),
    ],
    "asian": [
        ("Chicken Pad Thai", "main", 32, 50),
        ("Beef Stir Fry", "main", 38, 55),
        ("Chicken Fried Rice", "main", 28, 42),
        ("Sweet and Sour Chicken", "main", 32, 48),
        ("Spring Rolls (4 pcs)", "starter", 15, 25),
        ("Dumplings (6 pcs)", "starter", 22, 35),
        ("Hot and Sour Soup", "starter", 18, 28),
        ("Jasmine Rice", "side", 8, 15),
        ("Green Tea", "drink", 5, 10),
    ],
    "japanese": [
        ("California Roll (8 pcs)", "main", 35, 55),
        ("Salmon Sushi (6 pcs)", "main", 45, 70),
        ("Tuna Sushi (6 pcs)", "main", 42, 65),
        ("Mixed Sushi Platter", "main", 85, 130),
        ("Chicken Teriyaki", "main", 42, 62),
        ("Beef Yakiniku", "main", 65, 95),
        ("Miso Soup", "starter", 12, 22),
        ("Edamame", "starter", 15, 25),
        ("Green Tea Ice Cream", "dessert", 18, 28),
    ],
    "italian": [
        ("Spaghetti Bolognese", "main", 32, 50),
        ("Fettuccine Alfredo", "main", 35, 55),
        ("Pasta Carbonara", "main", 38, 58),
        ("Lasagna", "main", 42, 62),
        ("Chicken Parmesan", "main", 45, 65),
        ("Bruschetta", "starter", 18, 28),
        ("Caprese Salad", "starter", 22, 32),
        ("Tiramisu", "dessert", 22, 35),
        ("Cannoli", "dessert", 18, 28),
    ],
    "mexican": [
        ("Beef Tacos (3 pcs)", "main", 28, 42),
        ("Chicken Quesadilla", "main", 32, 48),
        ("Beef Burrito", "main", 35, 52),
        ("Nachos Supreme", "main", 32, 48),
        ("Guacamole and Chips", "starter", 22, 32),
        ("Mexican Rice", "side", 10, 18),
        ("Black Beans", "side", 10, 18),
    ],
    "turkish": [
        ("Lamb Kebab", "main", 55, 85),
        ("Chicken Kebab", "main", 42, 62),
        ("Iskender Kebab", "main", 65, 95),
        ("Adana Kebab", "main", 50, 75),
        ("Pide Cheese", "starter", 22, 35),
        ("Lentil Soup", "starter", 15, 25),
        ("Baklava (3 pcs)", "dessert", 18, 30),
        ("Turkish Tea", "drink", 5, 10),
        ("Turkish Coffee", "drink", 10, 18),
    ],
    "egyptian": [
        ("Koshary", "main", 18, 28),
        ("Molokhia with Chicken", "main", 32, 48),
        ("Foul Medames", "starter", 12, 22),
        ("Tameya (5 pcs)", "starter", 10, 18),
        ("Egyptian Bread", "side", 4, 8),
    ],
    "seafood": [
        ("Grilled Hammour", "main", 75, 120),
        ("Fried Shrimp", "main", 65, 95),
        ("Fish and Chips", "main", 45, 70),
        ("Seafood Platter", "main", 120, 180),
        ("Lobster Tail", "main", 145, 220),
        ("Shrimp Cocktail", "starter", 35, 55),
        ("Calamari Rings", "starter", 32, 48),
    ],
}

# Saudi/Arab first names (transliterated). Mix of common male and female names.
FIRST_NAMES_MALE = [
    "Mohammed", "Ahmed", "Abdullah", "Abdulrahman", "Khalid", "Faisal",
    "Saud", "Sultan", "Nasser", "Bandar", "Salman", "Fahad", "Turki",
    "Majed", "Yousef", "Omar", "Ali", "Hassan", "Hussein", "Ibrahim",
    "Ismail", "Saleh", "Saad", "Tariq", "Waleed", "Ziad", "Hamad",
    "Mansour", "Mishal", "Mutlaq", "Naif", "Rakan", "Sami", "Talal",
    "Walid", "Yazid", "Ammar", "Bader", "Dhari", "Fawaz",
]

FIRST_NAMES_FEMALE = [
    "Fatima", "Aisha", "Maryam", "Khadija", "Sarah", "Nora", "Hessa",
    "Lulu", "Latifa", "Munira", "Reem", "Lina", "Layla", "Hala", "Dana",
    "Nouf", "Rana", "Shahad", "Lama", "Mona", "Dalia", "Ghada", "Hanan",
    "Iman", "Jana", "Kholoud", "Maha", "Najla", "Ohood", "Rawan",
    "Salma", "Tala", "Wafa", "Yara", "Zahra",
]

# Saudi tribal/family surnames (Al- prefix is the standard transliteration)
SURNAMES = [
    "Al-Otaibi", "Al-Harbi", "Al-Ghamdi", "Al-Qahtani", "Al-Shammari",
    "Al-Subaie", "Al-Mutairi", "Al-Anazi", "Al-Dosari", "Al-Zahrani",
    "Al-Shehri", "Al-Maliki", "Al-Asmari", "Al-Bishi", "Al-Juhani",
    "Al-Saadi", "Al-Hashemi", "Al-Rashidi", "Al-Najjar", "Al-Khalidi",
    "Al-Sulaiman", "Al-Saleh", "Al-Faraj", "Al-Mansour", "Al-Khateeb",
    "Al-Saud", "Al-Sheikh", "Al-Qurashi", "Al-Tamimi", "Al-Yami",
    "Al-Asiri", "Al-Faifi", "Al-Hejailan", "Al-Jeraisy", "Al-Ruwais",
]

# Saudi review comments — mix of transliterated Arabic and English,
# positive / neutral / negative.
REVIEW_COMMENTS = [
    # Positive — English
    "Food was amazing, will order again!",
    "Great taste and fast delivery, thanks.",
    "Delicious as always, recommended.",
    "Best food in town, generous portions.",
    "Driver was very polite and on time.",
    "Hot and fresh, exactly as ordered.",
    "Excellent quality, my family loved it.",
    "Five stars, no complaints.",
    "Better than dining at the restaurant!",
    "Reasonable price for the portion.",
    "Will definitely become a regular customer.",
    "Packaging was perfect, nothing spilled.",

    # Positive — Arabic
    "ما شاء الله، الطعام ممتاز!",
    "والله لذيذ، تسلمون.",
    "بصراحة الأكل كان رائع جداً.",
    "الله يعطيكم العافية.",
    "من أحسن المطاعم في الحي.",
    "الطعام وصل بسرعة.",
    "الأكل يشهي، شكراً جزيلاً.",
    "سريع ولذيذ، ما شاء الله.",

    # Neutral
    "Food was okay, nothing special.",
    "Average meal, expected better for the price.",
    "Took longer than expected but food was fine.",
    "Decent but the portion was small.",
    "Acceptable quality.",
    "It's fine, but I've had better.",
    "Was good, though slightly cold when arrived.",
    "عادي، ما فيه شيء ممتاز ولا سيء.",
    "مو سيئ، لكن عادي جداً.",

    # Negative — English
    "Food arrived cold, very disappointing.",
    "Driver took over an hour, not acceptable.",
    "Order was missing items, please check next time.",
    "Quality has gone down recently.",
    "Wrong order delivered, had to wait for replacement.",
    "Too oily, not as I remembered.",
    "Portion much smaller than the picture.",
    "Packaging was damaged.",
    "Will not order again.",
    "The food was not fresh.",

    # Negative — Arabic
    "الأكل وصل بارد، للأسف.",
    "التوصيل تأخر ساعتين.",
    "ما فيه نفس المستوى السابق.",
    "ما التزموا بالتعليمات.",
    "الطعام ما كان طيب.",

    # Mixed / specific
    "Driver was nice but food was late.",
    "Tasty but missing the sauce I asked for.",
    "Good food, lukewarm by the time it arrived.",
]

# Operational notes occasionally attached to status events
STATUS_EVENT_NOTES = [
    None, None, None, None, None,  # mostly NULL
    None, None, None, None, None,
    "Heavy traffic, slight delay",
    "Restaurant prep delay",
    "Kitchen running behind",
    "Driver assigned",
    "Order ready ahead of schedule",
    "Confirmed by restaurant",
]

# Vehicle types for drivers (matches schema CHECK constraint)
VEHICLE_TYPES = ["bike", "scooter", "car"]

# Driver initial statuses (matches schema CHECK)
# We'll bias toward 'inactive' for most seeded drivers — they come online via the status process
DRIVER_INITIAL_STATUSES = ["inactive", "available", "offline"]

# Payment methods
PAYMENT_METHODS = ["card", "cash", "wallet", "bank_transfer"]
