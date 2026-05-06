"""TPC-H shared constants and data generation utilities"""

SHIP_MODES = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
SHIP_INSTRUCTS = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]
BRANDS = [f"Brand#{i}{j}" for i in range(1, 6) for j in range(1, 6)]
CONTAINERS = [
    f"{sz} {tp}"
    for sz in ["SM", "MED", "LG", "WRAP", "JUMBO"]
    for tp in ["CASE", "BOX", "PACK", "PKG", "BAG", "JAR", "CAN", "DRUM"]
]
PART_TYPES = [
    "ECONOMY ANODIZED STEEL", "ECONOMY BURNISHED COPPER",
    "ECONOMY PLATED STEEL", "ECONOMY BRUSHED TIN",
    "PROMO ANODIZED TIN", "PROMO BURNISHED STEEL",
    "PROMO PLATED NICKEL", "PROMO BRUSHED TIN",
    "STANDARD POLISHED BRASS", "STANDARD ANODIZED BRASS",
    "STANDARD BURNISHED COPPER", "STANDARD PLATED STEEL",
    "SMALL BRUSHED NICKEL", "SMALL PLATED TIN",
    "SMALL ANODIZED STEEL", "SMALL BURNISHED BRASS",
    "LARGE BURNISHED COPPER", "LARGE POLISHED NICKEL",
    "LARGE ANODIZED STEEL", "LARGE PLATED BRASS",
    "MEDIUM POLISHED COPPER", "MEDIUM BRUSHED TIN",
    "MEDIUM ANODIZED BRASS", "MEDIUM PLATED STEEL",
]
PART_COLORS = [
    "almond", "antique", "aquamarine", "azure", "beige",
    "bisque", "black", "blanched", "blue", "blush",
    "brown", "burlywood", "burnished", "chartreuse", "chiffon",
    "chocolate", "coral", "cornflower", "cornsilk", "cream",
    "cyan", "dark", "deep", "dim", "dodger",
    "drab", "firebrick", "floral", "forest", "frosted",
    "gainsboro", "ghost", "goldenrod", "green", "grey",
    "honeydew", "hot", "indian", "ivory", "khaki",
    "lace", "lavender", "lawn", "lemon", "light",
    "lime", "linen", "magenta", "maroon", "medium",
    "metallic", "midnight", "mint", "misty", "moccasin",
    "navajo", "navy", "olive", "orange", "orchid",
    "pale", "papaya", "peach", "peru", "pink",
    "plum", "powder", "puff", "purple", "red",
    "rose", "rosy", "royal", "saddle", "salmon",
    "sandy", "seashell", "sienna", "sky", "slate",
    "smoke", "snow", "spring", "steel", "tan",
    "thistle", "tomato", "turquoise", "violet", "wheat",
    "white", "yellow",
]
ORDER_PRIORITIES = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
ORDER_STATUS = ["O", "F", "P"]
SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
NATIONS = [
    "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT",
    "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA",
    "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
    "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA",
    "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES",
]
REGIONS = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]
NATION_TO_REGION = {
    "ALGERIA": "AFRICA", "ETHIOPIA": "AFRICA", "KENYA": "AFRICA",
    "MOROCCO": "AFRICA", "MOZAMBIQUE": "AFRICA",
    "ARGENTINA": "AMERICA", "BRAZIL": "AMERICA", "CANADA": "AMERICA",
    "PERU": "AMERICA", "UNITED STATES": "AMERICA",
    "CHINA": "ASIA", "INDIA": "ASIA", "INDONESIA": "ASIA",
    "JAPAN": "ASIA", "VIETNAM": "ASIA",
    "FRANCE": "EUROPE", "GERMANY": "EUROPE", "ROMANIA": "EUROPE",
    "RUSSIA": "EUROPE", "UNITED KINGDOM": "EUROPE",
    "EGYPT": "MIDDLE EAST", "IRAN": "MIDDLE EAST", "IRAQ": "MIDDLE EAST",
    "JORDAN": "MIDDLE EAST", "SAUDI ARABIA": "MIDDLE EAST",
}


def sql_array(items):
    """Convert Python list to Spark SQL array(...) expression"""
    return "array(" + ",".join(f"'{s}'" for s in items) + ")"
