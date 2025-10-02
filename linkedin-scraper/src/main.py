"""E-commerce Company & Contact Finder using Google Search and website scraping.

This approach:
1. Searches Google for e-commerce companies in Netherlands
2. Visits their websites to find contact information
3. Extracts emails using common patterns
4. Finds team/about pages for manager information

This is legal as it only accesses publicly available information.
"""

from __future__ import annotations

import asyncio
import re
from urllib.parse import quote_plus, urljoin, urlparse

from apify import Actor
from bs4 import BeautifulSoup
from httpx import AsyncClient


async def main() -> None:
    """Main entry point for the company contact finder."""
    async with Actor:
        # Get input configuration
        actor_input = await Actor.get_input() or {}
        
        search_queries = actor_input.get('search_queries', [
            'e-commerce manager Netherlands',
            'logistics manager Netherlands webshop',
            'marketing manager online retail Netherlands',
            'e-commerce bedrijf Nederland contact',
            'webshop Nederland team'
        ])
        
        max_companies = actor_input.get('max_companies', 50)
        
        Actor.log.info(f'Starting search for e-commerce companies and contacts')
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,nl;q=0.8',
        }
        
        all_contacts = []
        processed_domains = set()
        
        async with AsyncClient(headers=headers, timeout=30.0, follow_redirects=True) as client:
            
            for query in search_queries:
                if len(all_contacts) >= max_companies:
                    break
                    
                Actor.log.info(f'Searching Google: {query}')
                
                # Search Google
                companies = await search_google(client, query, max_results=20)
                
                for company in companies:
                    if len(all_contacts) >= max_companies:
                        break
                    
                    domain = urlparse(company['url']).netloc
                    if domain in processed_domains:
                        continue
                    
                    processed_domains.add(domain)
                    
                    Actor.log.info(f'Scraping: {company["name"]} - {company["url"]}')
                    
                    # Scrape company website
                    await asyncio.sleep(1)  # Be respectful
                    contact_info = await scrape_company_website(client, company)
                    
                    if contact_info:
                        all_contacts.append(contact_info)
                        await Actor.push_data([contact_info])  # Save incrementally
        
        Actor.log.info(f'Successfully found {len(all_contacts)} companies with contact information')


async def search_google(client: AsyncClient, query: str, max_results: int = 20) -> list[dict]:
    """Search Google and extract company websites."""
    companies = []
    
    try:
        search_url = f'https://www.google.com/search?q={quote_plus(query)}&num={max_results}'
        response = await client.get(search_url)
        soup = BeautifulSoup(response.content, 'lxml')
        
        # Find search result divs
        results = soup.find_all('div', class_='g')
        
        for result in results:
            try:
                # Extract title and URL
                link_elem = result.find('a')
                if not link_elem or not link_elem.get('href'):
                    continue
                
                url = link_elem['href']
                
                # Skip non-http links
                if not url.startswith('http'):
                    continue
                
                # Skip Google's own pages and common non-company sites
                domain = urlparse(url).netloc.lower()
                skip_domains = ['google.', 'youtube.', 'facebook.', 'linkedin.', 'twitter.', 'instagram.']
                if any(skip in domain for skip in skip_domains):
                    continue
                
                # Extract title
                title_elem = result.find('h3')
                title = title_elem.get_text(strip=True) if title_elem else domain
                
                companies.append({
                    'name': title,
                    'url': url
                })
                
            except Exception as e:
                Actor.log.debug(f'Error parsing search result: {e}')
                continue
        
        Actor.log.info(f'Found {len(companies)} potential companies from Google')
        
    except Exception as e:
        Actor.log.error(f'Error searching Google: {e}')
    
    return companies


async def scrape_company_website(client: AsyncClient, company: dict) -> dict | None:
    """Scrape company website for contact information."""
    
    result = {
        'company_name': company['name'],
        'website': company['url'],
        'emails': [],
        'phones': [],
        'linkedin': None,
        'team_members': [],
        'about_url': None,
        'contact_url': None
    }
    
    try:
        # Get homepage
        response = await client.get(company['url'])
        soup = BeautifulSoup(response.content, 'lxml')
        
        # Extract all text for email/phone pattern matching
        page_text = soup.get_text()
        
        # Find emails using regex
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, page_text)
        result['emails'] = list(set(emails))[:10]  # Limit to 10 unique emails
        
        # Find phone numbers (Dutch format)
        phone_pattern = r'(\+31|0031|0)[\s.-]?(\d[\s.-]?){8,9}\d'
        phones = re.findall(phone_pattern, page_text)
        result['phones'] = list(set([''.join(p) for p in phones]))[:5]
        
        # Find LinkedIn company page
        linkedin_links = soup.find_all('a', href=re.compile(r'linkedin\.com/company/', re.I))
        if linkedin_links:
            result['linkedin'] = linkedin_links[0]['href']
        
        # Find important pages
        all_links = soup.find_all('a', href=True)
        
        for link in all_links:
            href = link['href'].lower()
            text = link.get_text().lower()
            
            # Look for team/about pages
            if any(keyword in href or keyword in text for keyword in ['team', 'about', 'over-ons', 'about-us']):
                about_url = urljoin(company['url'], link['href'])
                if not result['about_url']:
                    result['about_url'] = about_url
                    # Scrape team page
                    await asyncio.sleep(1)
                    team_members = await scrape_team_page(client, about_url)
                    result['team_members'] = team_members
            
            # Look for contact pages
            if any(keyword in href or keyword in text for keyword in ['contact', 'contacteer']):
                if not result['contact_url']:
                    result['contact_url'] = urljoin(company['url'], link['href'])
        
        # Only return if we found at least some contact info
        if result['emails'] or result['phones'] or result['team_members']:
            return result
        
    except Exception as e:
        Actor.log.debug(f'Error scraping {company["url"]}: {e}')
    
    return None


async def scrape_team_page(client: AsyncClient, url: str) -> list[dict]:
    """Scrape team/about page for manager information."""
    team_members = []
    
    try:
        response = await client.get(url, timeout=20.0)
        soup = BeautifulSoup(response.content, 'lxml')
        
        # Look for common patterns in team pages
        # This is a simplified approach - adjust based on actual website structures
        
        # Pattern 1: Look for divs/sections with person info
        team_sections = soup.find_all(['div', 'section', 'article'], 
                                     class_=re.compile(r'team|member|employee|staff|person', re.I))
        
        for section in team_sections[:20]:  # Limit to avoid over-scraping
            member = {}
            
            # Try to find name
            name_elem = section.find(['h2', 'h3', 'h4', 'strong', 'b'])
            if name_elem:
                member['name'] = name_elem.get_text(strip=True)
            
            # Try to find title/position
            text = section.get_text()
            
            # Look for manager titles
            manager_keywords = ['manager', 'director', 'head of', 'ceo', 'cmo', 'coo', 
                              'e-commerce', 'marketing', 'logistics', 'operations']
            
            if any(keyword in text.lower() for keyword in manager_keywords):
                # Extract position
                lines = [line.strip() for line in text.split('\n') if line.strip()]
                if len(lines) >= 2:
                    member['position'] = lines[1] if member.get('name') == lines[0] else lines[0]
                
                # Look for email in this section
                email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                emails = re.findall(email_pattern, text)
                if emails:
                    member['email'] = emails[0]
                
                if member.get('name'):
                    team_members.append(member)
        
    except Exception as e:
        Actor.log.debug(f'Error scraping team page {url}: {e}')
    
    return team_members