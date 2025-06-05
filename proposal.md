# Capstone Project Proposal

## Student Information*
Name: Yin Vang
Email: hoyinvang@gmail.com

## Project Overview*
Project Title: Data-Infused  
Domain Area: Retail Inventory & Consumer Intelligence  
One-Line Description: A digital warehousing and analytics system for tea retailers that integrates real-time stock flow and customer feedback to drive smarter restocking and sales strategies.

## Technical Components*

### Data Sources*

1. Real-time Source:  
   - Description: Simulated API stream that generates ongoing tea-related transactions based on the UCI Online Retail dataset structure.  
   - Update frequency: Every few minutes (script-controlled)  
   - Estimated volume: 100–300 records/day  
   - Access method: Python script simulating REST API responses

2. Batch Source:  
   - Description: Historical invoice-level transaction data from the UCI Online Retail dataset  
   - Update frequency: One-time load with optional weekly refresh  
   - Estimated volume: 500,000+ rows (filtered to tea-related products via keyword matching)  
   - Data format: CSV  
   - Purpose: Provides a foundational dataset for modeling tea product movement and sales behavior in batch

3. Document Processing:  
   - File type(s): Product reviews from the Kaggle “Herbal Tea Reviews from Amazon” dataset  
   - Processing needs: Extract tea name, rating, sentiment, and review context  
   - Expected volume: 3,000+ reviews  
   - Note: Document processing and embeddings will begin in Module 3

### Data Model
Fact Tables:
- tea_transactions (transaction_id, tea_id, quantity, timestamp, customer_id, country)

Dimension Tables:
- teas (tea_id, name, category, origin)
- customers (customer_id, region, loyalty_score)
- time (date_id, day, week, month, year)
- reviews (review_id, tea_id, rating, sentiment, embedding_vector_id)

### AI Component
Query Types:
- “Which teas are most popular and out of stock?”
- “What does customer sentiment say about black teas?”
- “Which teas should be promoted next month based on ratings and sales trends?”

Document Processing Approach:
- Extract review texts, ratings, and metadata
- Embed into vector DB for semantic search
- Use LLM to summarize product sentiment

RAG Implementation Plan:
- Build document knowledge base from Kaggle reviews
- Use LangChain to query both vector DB + warehouse data
- Generate human-readable insights via OpenAI API

### Extra Features (Optional)
Planned additional features:
- Stock alert system based on real-time inventory changes and velocity thresholds
- Tea popularity dashboard combining customer reviews and sales data
- Promotion recommender engine powered by sentiment and transaction patterns

## Implementation Timeline*
Module 1:
- Ingest UCI retail data (batch + simulated stream)  
- Filter for tea-related SKUs  
- Initial schema design  
- Submit proposal and build Airflow pipeline  

Module 2:
- Apply transformations and modeling with dbt  
- Implement basic dbt tests for quality assurance  
- Create warehouse tables with fact/dim structure  
- Simulate updates via batch refresh  

Module 3:
- Integrate Kaggle tea reviews  
- Document processing + embedding  
- Vector DB setup and RAG system prototype  

Module 4:
- Final AI chatbot + dashboards  
- Add alerts and polishing features  
- Prepare final presentation  

## Technical Considerations

Known Challenges:
- Mapping ambiguous or inconsistent product names in the UCI dataset to actual tea types
- Cleaning and structuring Amazon reviews for embedding (especially mixed formats or noisy data)
- Ensuring meaningful semantic search for tea-related questions with minimal review volume
- Managing local development vs. scalable deployment (e.g., FAISS now, Pinecone later)

Support Needed:
- Best practices for setting up vector search and embedding quality
- Guidance on orchestrating ELT DAGs in Airflow with retry/error handling
- Advice on how to simulate streaming tea transactions realistically

Required Resources:
- Access to OpenAI or HuggingFace embeddings
- Tesseract or BeautifulSoup for document scraping/parsing
- Airflow setup (local or hosted)
- FAISS (for local vector database) or Pinecone account (if upgraded later)

## Additional Notes
This project demonstrates how merging operational and experiential data can unlock smarter retail decision-making — an extensible solution for any product-based business beyond tea.

