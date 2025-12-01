"""
Enhanced AI Chat Manager with User Memory Integration
Complete implementation with user profile awareness and conversation memory
"""
import os
import logging
import json
from typing import Dict, List, Optional
from datetime import datetime
from openai import AzureOpenAI
import aiohttp
from bs4 import BeautifulSoup
import asyncio

from config import Config

logger = logging.getLogger(__name__)

class EnhancedAIChatManager:
    """Enhanced AI chat manager with user memory and personalization"""
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.client = None
        self.deployment_name = Config.DEPLOYMENT_NAME  # Fixed: was AZURE_OPENAI_DEPLOYMENT
        self.api_version = Config.AZURE_OPENAI_API_VERSION
        
        # Enhanced system prompt with user awareness
        self.system_prompt = """You are STRETCH Bot, an intelligent assistant for STRETCH, a professional stretch ceiling installation company based in Belgium.

Your role is to:
1. Provide personalized assistance based on the user's profile and history
2. Help users understand stretch ceiling options, benefits, and pricing
3. Guide them through the quote creation process
4. Answer technical questions about products and installation
5. Remember user preferences and past interactions

Key behaviors:
- Address users by name when their profile is available
- Reference their company details when relevant
- Remember their preferences from past conversations
- Provide pricing appropriate to their client type (B2C, B2B Reseller, B2B Hospitality)
- Be proactive in offering relevant suggestions based on their history

When discussing pricing:
- B2C customers get standard retail pricing
- B2B Resellers get discounted rates for bulk orders
- B2B Hospitality clients get specialized commercial pricing

Always maintain a professional, helpful, and personalized approach. If users need to create quotes, guide them to use /create_quote. For viewing existing quotes, suggest /quotes.

Base your responses on the user's profile information, conversation history, and preferences provided in the context."""
        
        # Initialize Azure OpenAI client
        self._initialize_client()
        
        # Website data cache
        self.website_data = {}
        self.website_data_loaded = False
    
    def _initialize_client(self):
        """Initialize Azure OpenAI client"""
        try:
            if all([
                Config.AZURE_OPENAI_API_KEY,
                Config.AZURE_OPENAI_ENDPOINT,
                Config.DEPLOYMENT_NAME,  # Fixed: was AZURE_OPENAI_DEPLOYMENT
                Config.AZURE_OPENAI_API_VERSION
            ]):
                self.client = AzureOpenAI(
                    api_key=Config.AZURE_OPENAI_API_KEY,
                    api_version=self.api_version,
                    azure_endpoint=Config.AZURE_OPENAI_ENDPOINT
                )
                logger.info("✅ Azure OpenAI client initialized successfully")
            else:
                logger.warning("⚠️ Azure OpenAI configuration incomplete")
                
        except Exception as e:
            logger.error(f"❌ Error initializing Azure OpenAI: {e}")
            self.client = None
    
    def get_status(self) -> Dict:
        """Get AI service status"""
        return {
            "client_initialized": self.client is not None,
            "api_key_configured": bool(Config.AZURE_OPENAI_API_KEY),
            "endpoint_configured": bool(Config.AZURE_OPENAI_ENDPOINT),
            "deployment_name": self.deployment_name,
            "api_version": self.api_version,
            "website_data_loaded": self.website_data_loaded
        }
    
    def log_conversation(self, user_id: int, message_type: str, message: str, context: Dict = None):
        """Log conversation to database"""
        self.db.log_conversation(user_id, message_type, message, context)
    
    async def get_user_context_prompt(self, user_id: int) -> str:
        """Build context prompt with user information and memory"""
        # Get user profile
        user_profile = self.db.get_user_profile(user_id)
        
        # Get conversation memory
        memory = self.db.get_user_conversation_memory(user_id)
        
        # Get recent quotes
        recent_quotes = self.db.get_user_quotes(user_id)[:3]  # Last 3 quotes
        
        context_prompt = ""
        
        # Add user profile information
        if user_profile and user_profile.get('onboarding_completed'):
            context_prompt += f"\n\nUser Profile Information:\n"
            context_prompt += f"- Name: {user_profile['first_name']} {user_profile.get('last_name', '')}\n"
            
            if user_profile.get('is_company'):
                context_prompt += f"- Company: {user_profile['company_name']}\n"
                context_prompt += f"- VAT Number: {user_profile['vat_number']}\n"
            
            context_prompt += f"- Email: {user_profile.get('email', 'Not provided')}\n"
            context_prompt += f"- Phone: {user_profile.get('phone', 'Not provided')}\n"
            context_prompt += f"- Address: {user_profile.get('address', 'Not provided')}\n"
            context_prompt += f"- Client Type: {user_profile.get('client_group', 'B2C').replace('price_', '').upper()}\n"
            
            # Add user preferences if available
            if user_profile.get('preferences'):
                context_prompt += f"\nUser Preferences:\n"
                for key, value in user_profile['preferences'].items():
                    context_prompt += f"- {key}: {value}\n"
        
        # Add conversation memory
        if memory and memory['interaction_count'] > 0:
            context_prompt += f"\n\nConversation History Summary:\n"
            
            if memory.get('conversation_summary'):
                context_prompt += f"{memory['conversation_summary']}\n"
            
            if memory.get('preferences_learned'):
                context_prompt += f"\nLearned Preferences:\n"
                for key, value in memory['preferences_learned'].items():
                    context_prompt += f"- {key}: {value}\n"
            
            if memory.get('last_topics') and len(memory['last_topics']) > 0:
                context_prompt += f"\nRecent Topics Discussed: {', '.join(memory['last_topics'])}\n"
            
            context_prompt += f"\nTotal Interactions: {memory['interaction_count']}\n"
        
        # Add recent quotes information
        if recent_quotes:
            context_prompt += f"\n\nRecent Quotes:\n"
            for quote in recent_quotes:
                quote_data = json.loads(quote['quote_data'])
                context_prompt += f"- Quote #{quote['quotation_id']} ({quote['created_at'].strftime('%Y-%m-%d')}): "
                context_prompt += f"€{quote['total_price']:.2f}, {len(quote_data.get('ceilings', []))} ceiling(s)"
                
                if quote_data.get('quote_reference'):
                    context_prompt += f", Ref: {quote_data['quote_reference']}"
                context_prompt += f", Status: {quote['status']}\n"
        
        return context_prompt
    
    async def get_ai_response(self, user_id: int, message: str) -> str:
        """Get AI response with enhanced user context"""
        try:
            # Log the conversation
            self.log_conversation(user_id, "user", message)
            
            # Get user context
            user_context = await self.get_user_context_prompt(user_id)
            
            # Get conversation history with this user
            chat_history = self.db.get_conversation_history(user_id, limit=10)
            
            # Build conversation history for the AI
            messages = [
                {
                    "role": "system",
                    "content": self.system_prompt + user_context
                }
            ]
            
            # Add website data context if available
            if self.website_data_loaded and self.website_data:
                website_context = "\n\nCompany Information from Website:\n"
                for key, value in self.website_data.items():
                    if isinstance(value, str) and len(value) < 500:  # Limit context size
                        website_context += f"- {key}: {value}\n"
                messages[0]["content"] += website_context
            
            # Add recent conversation history
            for log in reversed(chat_history):  # Reverse to get chronological order
                if log['message_type'] == 'user':
                    messages.append({"role": "user", "content": log['message']})
                elif log['message_type'] == 'bot':
                    # Only add bot messages that are AI responses
                    if not log['message'].startswith('/') and 'Welcome to' not in log['message']:
                        messages.append({"role": "assistant", "content": log['message']})
            
            # Add current message
            messages.append({"role": "user", "content": message})
            
            # Keep only last N messages to avoid token limits
            if len(messages) > 15:
                # Keep system message and last 14 messages
                messages = [messages[0]] + messages[-14:]
            
            # Call AI
            if self.client:
                response = self.client.chat.completions.create(
                    model=self.deployment_name,
                    messages=messages,
                    temperature=0.7,
                    max_tokens=800,
                    top_p=0.95,
                    frequency_penalty=0,
                    presence_penalty=0
                )
                
                ai_response = response.choices[0].message.content
            else:
                # Enhanced fallback response that still uses user context
                user_name = ""
                if user_context:
                    # Extract name from context
                    import re
                    name_match = re.search(r"- Name: (.+)\n", user_context)
                    if name_match:
                        user_name = name_match.group(1).split()[0]  # First name only
                
                ai_response = self._get_personalized_fallback_response(message, user_name)
            
            # Log the AI response
            self.log_conversation(user_id, "bot", ai_response)
            
            # Update conversation memory
            await self.update_user_conversation_memory(user_id, message, ai_response)
            
            return ai_response
            
        except Exception as e:
            logger.error(f"Error getting AI response: {e}")
            return self._get_fallback_response(message)
    
    def _get_personalized_fallback_response(self, message: str, user_name: str = "") -> str:
        """Get personalized fallback response when AI is unavailable"""
        message_lower = message.lower()
        
        greeting = f"Hello {user_name}! " if user_name else "Hello! "
        
        if any(word in message_lower for word in ["price", "cost", "how much", "quote", "estimate"]):
            return f"{greeting}For accurate pricing, I recommend creating a quote using /create_quote. Our prices vary based on the type of ceiling, size, and additional features you choose."
        
        elif any(word in message_lower for word in ["install", "installation", "how long"]):
            return f"{greeting}Installation typically takes 1-2 days depending on the project size. Our professional installers ensure a perfect finish. Would you like to create a quote to get started?"
        
        elif any(word in message_lower for word in ["type", "kind", "option", "choice"]):
            return f"{greeting}We offer fabric and PVC stretch ceilings in various colors and finishes. Each type has unique benefits. Use /create_quote to explore options for your space."
        
        elif any(word in message_lower for word in ["acoustic", "sound", "noise"]):
            return f"{greeting}Our acoustic stretch ceilings can significantly reduce echo and improve sound quality. They're perfect for offices, restaurants, and homes. Want to learn more? Create a quote with /create_quote."
        
        elif any(word in message_lower for word in ["maintain", "clean", "maintenance", "care"]):
            return f"{greeting}Stretch ceilings are very low maintenance! They can be cleaned with a soft cloth and mild soap. They're also moisture-resistant and don't collect dust. Would you like to know more about our products?"
        
        elif any(word in message_lower for word in ["warranty", "guarantee", "how long last"]):
            return f"{greeting}Our stretch ceilings come with a 10-year warranty on materials and 2 years on installation. They're designed to last for decades with proper care. Ready to get a quote?"
        
        elif any(word in message_lower for word in ["hello", "hi", "hey", "good morning", "good afternoon"]):
            return f"{greeting}How can I help you today? I can provide information about stretch ceilings, help you create a quote, or answer any questions you have."
        
        elif any(word in message_lower for word in ["thank", "thanks", "appreciate"]):
            return f"You're welcome{', ' + user_name if user_name else ''}! Is there anything else I can help you with?"
        
        else:
            return f"{greeting}I'm here to help with your stretch ceiling needs. You can:\n\n• Create a quote: /create_quote\n• View your quotes: /quotes\n• Get support: /help\n\nWhat would you like to do?"
    
    def _get_fallback_response(self, message: str) -> str:
        """Get basic fallback response"""
        return self._get_personalized_fallback_response(message, "")
    
    async def update_user_conversation_memory(self, user_id: int, user_message: str, ai_response: str):
        """Update user's conversation memory with new interaction"""
        try:
            # Get current memory
            memory = self.db.get_user_conversation_memory(user_id)
            
            # Update interaction count
            memory['interaction_count'] = memory.get('interaction_count', 0) + 1
            
            # Extract topics from the conversation
            topics = self._extract_topics(user_message + " " + ai_response)
            
            # Update last topics (keep last 10)
            last_topics = memory.get('last_topics', [])
            for topic in topics:
                if topic not in last_topics:
                    last_topics.append(topic)
            memory['last_topics'] = last_topics[-10:]
            
            # Extract any preferences mentioned
            preferences = self._extract_preferences(user_message, ai_response)
            if preferences:
                current_prefs = memory.get('preferences_learned', {})
                current_prefs.update(preferences)
                memory['preferences_learned'] = current_prefs
            
            # Update conversation summary (simple approach - can be enhanced)
            if memory['interaction_count'] % 5 == 0:  # Update summary every 5 interactions
                # In a real implementation, you might use AI to generate a summary
                memory['conversation_summary'] = f"User has had {memory['interaction_count']} interactions. " \
                                                f"Recent topics: {', '.join(memory['last_topics'][-5:])}."
            
            # Add key points from this conversation
            key_points = memory.get('key_points', [])
            if any(word in user_message.lower() for word in ['want', 'need', 'looking for', 'interested in']):
                key_points.append({
                    'date': datetime.now().isoformat(),
                    'point': user_message[:100]
                })
                # Keep only last 20 key points
                memory['key_points'] = key_points[-20:]
            
            # Save updated memory
            self.db.update_user_conversation_memory(user_id, memory)
            
        except Exception as e:
            logger.error(f"Error updating conversation memory: {e}")
    
    def _extract_topics(self, text: str) -> List[str]:
        """Extract main topics from conversation text"""
        topics = []
        
        # Define topic keywords
        topic_keywords = {
            'pricing': ['price', 'cost', 'quote', 'estimate', 'budget', 'expensive', 'cheap', 'afford'],
            'installation': ['install', 'installation', 'setup', 'mounting', 'fitting', 'installer'],
            'acoustic': ['acoustic', 'sound', 'noise', 'echo', 'quiet', 'soundproof'],
            'lighting': ['light', 'lights', 'LED', 'illumination', 'spotlight', 'lamp'],
            'design': ['design', 'color', 'style', 'aesthetic', 'look', 'appearance', 'beautiful'],
            'maintenance': ['clean', 'maintain', 'maintenance', 'care', 'wash', 'dirt'],
            'warranty': ['warranty', 'guarantee', 'protection', 'insurance', 'coverage'],
            'fabric': ['fabric', 'textile', 'cloth', 'material'],
            'pvc': ['pvc', 'vinyl', 'plastic'],
            'measurement': ['size', 'dimension', 'area', 'meter', 'length', 'width'],
            'company': ['company', 'business', 'commercial', 'office', 'restaurant', 'hotel']
        }
        
        text_lower = text.lower()
        for topic, keywords in topic_keywords.items():
            if any(keyword in text_lower for keyword in keywords):
                topics.append(topic)
        
        return topics
    
    def _extract_preferences(self, user_message: str, ai_response: str) -> Dict[str, str]:
        """Extract user preferences from conversation"""
        preferences = {}
        
        message_lower = user_message.lower()
        
        # Extract color preferences
        colors = ['white', 'black', 'grey', 'beige', 'cream', 'blue', 'red', 'green', 'yellow', 'brown']
        for color in colors:
            if color in message_lower and any(word in message_lower for word in ['like', 'prefer', 'want', 'love']):
                preferences['preferred_color'] = color
                break
        
        # Extract material preferences
        if 'fabric' in message_lower and any(word in message_lower for word in ['prefer', 'want', 'like', 'need']):
            preferences['preferred_material'] = 'fabric'
        elif 'pvc' in message_lower and any(word in message_lower for word in ['prefer', 'want', 'like', 'need']):
            preferences['preferred_material'] = 'pvc'
        
        # Extract budget indicators
        if any(word in message_lower for word in ['budget', 'cheap', 'affordable', 'save money', 'cost effective']):
            preferences['budget_conscious'] = 'yes'
        elif any(word in message_lower for word in ['premium', 'best', 'quality', 'luxury', 'high end']):
            preferences['quality_focused'] = 'yes'
        
        # Extract room type preferences
        room_types = ['bedroom', 'living room', 'kitchen', 'bathroom', 'office', 'restaurant', 'hotel']
        for room in room_types:
            if room in message_lower:
                preferences['interested_in_room'] = room
                break
        
        # Extract timing preferences
        if any(word in message_lower for word in ['urgent', 'asap', 'quickly', 'fast', 'hurry']):
            preferences['timeline'] = 'urgent'
        elif any(word in message_lower for word in ['planning', 'future', 'next year', 'considering']):
            preferences['timeline'] = 'planning'
        
        return preferences
    
    async def scrape_website_data(self):
        """Scrape company website for additional context"""
        if not Config.COMPANY_WEBSITE:
            logger.warning("No company website configured")
            return
        
        try:
            logger.info(f"🌐 Scraping website data from {Config.COMPANY_WEBSITE}")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(Config.COMPANY_WEBSITE, timeout=10) as response:
                    if response.status == 200:
                        html = await response.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # Extract relevant information
                        self.website_data = {
                            'title': soup.title.string if soup.title else '',
                            'description': '',
                            'services': [],
                            'contact_info': {}
                        }
                        
                        # Extract meta description
                        meta_desc = soup.find('meta', attrs={'name': 'description'})
                        if meta_desc:
                            self.website_data['description'] = meta_desc.get('content', '')
                        
                        # Extract services (customize based on website structure)
                        service_elements = soup.find_all(['h2', 'h3'], class_=['service', 'product'])
                        self.website_data['services'] = [elem.text.strip() for elem in service_elements[:10]]
                        
                        # Save to database
                        self.db.save_website_data(self.website_data)
                        self.website_data_loaded = True
                        
                        logger.info("✅ Website data scraped successfully")
                    else:
                        logger.warning(f"Failed to scrape website: HTTP {response.status}")
                        
        except Exception as e:
            logger.error(f"Error scraping website: {e}")
            # Try to load from database
            self.load_website_data_from_db()
    
    def load_website_data_from_db(self):
        """Load website data from database"""
        try:
            stored_data = self.db.execute_query(
                "SELECT website_data FROM ai_chat_contexts WHERE user_id = 0",
                fetch=True
            )
            
            if stored_data and stored_data[0].get('website_data'):
                self.website_data = json.loads(stored_data[0]['website_data'])
                self.website_data_loaded = True
                logger.info("✅ Loaded website data from database")
                
        except Exception as e:
            logger.error(f"Error loading website data from database: {e}")
    
    async def generate_conversation_summary(self, user_id: int) -> str:
        """Generate a summary of user's conversation history using AI"""
        if not self.client:
            return ""
        
        try:
            # Get recent conversations
            conversations = self.db.get_conversation_history(user_id, limit=20)
            
            if not conversations:
                return ""
            
            # Build conversation text
            conv_text = "Recent conversation history:\n"
            for conv in reversed(conversations):
                role = "User" if conv['message_type'] == 'user' else "Assistant"
                conv_text += f"{role}: {conv['message']}\n"
            
            # Ask AI to summarize
            messages = [
                {
                    "role": "system",
                    "content": "You are a helpful assistant. Summarize the following conversation history in 2-3 sentences, focusing on the user's main interests and needs."
                },
                {
                    "role": "user",
                    "content": conv_text
                }
            ]
            
            response = self.client.chat.completions.create(
                model=self.deployment_name,
                messages=messages,
                temperature=0.5,
                max_tokens=150
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error generating conversation summary: {e}")
            return ""