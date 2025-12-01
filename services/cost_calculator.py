"""
Cost Calculator for Stretch Ceiling Bot
Fixed version with product_code field handling
"""
import logging
from models import CeilingConfig, CeilingCost

logger = logging.getLogger(__name__)

class CostCalculator:
    """Handles all cost calculations"""
    
    def __init__(self, db_manager):
        self.db = db_manager
    
    def calculate_ceiling_costs(self, config: CeilingConfig, client_group: str) -> CeilingCost:
        """Calculate all costs for a single ceiling"""
        costs = CeilingCost()
        
        # Fix client group mapping - normalize format
        if not client_group.startswith("price_"):
            price_column = {
                "b2c": "price_b2c",
                "b2b_reseller": "price_b2b_reseller",
                "b2b_hospitality": "price_b2b_hospitality",
            }.get(client_group, "price_b2c")
        else:
            price_column = client_group
        
        logger.info(f"💰 Calculating costs using price column: {price_column}")
        logger.info(f"📐 Ceiling config: {config.name} - {config.ceiling_type}/{config.type_ceiling}/{config.color}")
        logger.info(f"📏 Dimensions: {config.length}m × {config.width}m = {config.area}m²")
        logger.info(f"📏 Perimeter: {config.perimeter}m" + (" (manually edited)" if config.perimeter_edited else ""))
        
        # 1. Ceiling cost
        ceiling_product = self.db.get_ceiling_product(config.ceiling_type, config.type_ceiling, config.color)
        
        if ceiling_product:
            # Get price with fallback
            ceiling_price = float(ceiling_product.get(price_column, 0))
            
            # If no price in requested column, try others
            if ceiling_price <= 0:
                for fallback in ["price_b2c", "price_b2b_reseller", "price_b2b_hospitality"]:
                    ceiling_price = float(ceiling_product.get(fallback, 0))
                    if ceiling_price > 0:
                        logger.warning(f"⚠️ Using fallback price from {fallback}: €{ceiling_price}")
                        break
            
            if ceiling_price > 0:
                costs.ceiling_cost = config.area * ceiling_price
                logger.info(f"✅ Ceiling: {config.area}m² × €{ceiling_price}/m² = €{costs.ceiling_cost:.2f}")
            else:
                # Use default price
                default_price = 35.0
                costs.ceiling_cost = config.area * default_price
                logger.warning(
                    f"⚠️ No price found, using default: {config.area}m² × €{default_price}/m² = €{costs.ceiling_cost:.2f}"
                )
        else:
            # No product found - use default
            default_price = 35.0
            costs.ceiling_cost = config.area * default_price
            logger.warning(
                f"⚠️ No product found, using default: {config.area}m² × €{default_price}/m² = €{costs.ceiling_cost:.2f}"
            )
        
        # 2. Perimeter structure cost (S Plafond 12245)
        perimeter_structure = self.db.get_product_by_code("S Plafond 12245")
        if perimeter_structure:
            perimeter_price = float(perimeter_structure.get(price_column, 0))
            if perimeter_price > 0:
                costs.perimeter_structure_cost = config.perimeter * perimeter_price
                logger.info(
                    f"Perimeter structure: {
                    config.perimeter}m × €{perimeter_price} = €{
                    costs.perimeter_structure_cost:.2f}"
                )
        
        # 3. Perimeter profile cost
        if config.perimeter_profile:
            profile_price = float(config.perimeter_profile.get(price_column, 0))
            if profile_price > 0:
                costs.perimeter_profile_cost = config.perimeter * profile_price
                logger.info(
                    f"Perimeter profile: {
                    config.perimeter}m × €{profile_price} = €{
                    costs.perimeter_profile_cost:.2f}"
                )
        
        # 4. Corners cost (S Plafond 190)
        corner_product = self.db.get_product_by_code("S Plafond 190")
        if corner_product:
            corner_price = float(corner_product.get(price_column, 0))
            if corner_price > 0:
                costs.corners_cost = config.corners * corner_price
                logger.info(f"Corners: {config.corners} × €{corner_price} = €{costs.corners_cost:.2f}")
        
        # 5. Seam cost (S Plafond 13869)
        if config.has_seams and config.seam_length > 0:
            seam_product = self.db.get_product_by_code("S Plafond 13869")
            if seam_product:
                seam_price = float(seam_product.get(price_column, 0))
                if seam_price > 0:
                    costs.seam_cost = config.seam_length * seam_price
                    logger.info(f"Seams: {config.seam_length}m × €{seam_price} = €{costs.seam_cost:.2f}")
        
        # 6. Lights cost - FIXED to handle both 'code' and 'product_code'
        if config.lights:
            logger.info(f"💡 Calculating costs for {len(config.lights)} lights")
            for light in config.lights:
                # Handle both 'code' and 'product_code' for backward compatibility
                light_code = light.get('product_code') or light.get('code', 'UNKNOWN')
                light_quantity = light.get('quantity', 0)
                light_price = light.get('price', 0) or light.get(price_column, 0)
                light_total = light_quantity * float(light_price)
                costs.lights_cost += light_total
                logger.info(f"Light {light_code}: {light_quantity} × €{light_price} = €{light_total:.2f}")
        
        # 7. Wood structures cost - FIXED to handle both 'code' and 'product_code'
        if config.wood_structures:
            logger.info(f"🪵 Calculating costs for {len(config.wood_structures)} wood structures")
            for wood in config.wood_structures:
                # Handle both 'code' and 'product_code' for backward compatibility
                wood_code = wood.get('product_code') or wood.get('code', 'UNKNOWN')
                wood_quantity = wood.get('quantity', 0)
                wood_price = wood.get('price', 0) or wood.get(price_column, 0)
                wood_total = wood_quantity * float(wood_price)
                costs.wood_structures_cost += wood_total
                logger.info(f"Wood {wood_code}: {wood_quantity}m × €{wood_price} = €{wood_total:.2f}")
        
        # 8. Acoustic absorber cost
        if config.acoustic_product:
            absorber_price = float(config.acoustic_product.get(price_column, 0))
            if absorber_price > 0:
                costs.acoustic_absorber_cost = config.area * absorber_price
                logger.info(
                    f"Acoustic absorber: {
                    config.area}m² × €{absorber_price} = €{
                    costs.acoustic_absorber_cost:.2f}"
                )
        
        logger.info(f"💰 TOTAL COST: €{costs.total:.2f}")
        return costs