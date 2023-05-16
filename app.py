from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from bson.objectid import ObjectId
from pydantic import BaseModel
from typing import List

import uvicorn

# create the FastAPI instance
app = FastAPI()


# create the Pydantic model for books
class Book(BaseModel):
    title: str
    author: str
    description: str
    price: float
    stock: int
    items_sold: int = 0


# create the MongoDB client
client = MongoClient('mongodb://localhost:27017')
db = client['bookstore']
collection = db['books']


# define the API endpoints
@app.get('/books', response_model=List[Book])
async def get_books():
    books = collection.find()
    return [Book(**book) for book in books]


@app.get('/books/{book_id}', response_model=Book)
async def get_book(book_id: str):
    book = collection.find_one({'_id': ObjectId(book_id)})
    if book:
        return Book(**book)
    else:
        return JSONResponse(content={'error': 'Book not found'}, status_code=404)


@app.post('/books', response_model=Book)
async def add_book(book: Book):
    book_dict = book.dict()
    result = collection.insert_one(book_dict)
    book_dict['_id'] = result.inserted_id
    return Book(**book_dict)


@app.put('/books/{book_id}', response_model=Book)
async def update_book(book_id: str, book: Book):
    book_dict = book.dict()
    result = collection.replace_one({'_id': ObjectId(book_id)}, book_dict)
    if result.modified_count == 1:
        book_dict['_id'] = ObjectId(book_id)
        return Book(**book_dict)
    else:
        return JSONResponse(content={'error': 'Book not found'}, status_code=404)


@app.delete('/books/{book_id}')
async def delete_book(book_id: str):
    result = collection.delete_one({'_id': ObjectId(book_id)})
    if result.deleted_count == 1:
        return JSONResponse(content={'success': 'Book deleted'}, status_code=200)
    else:
        return JSONResponse(content={'error': 'Book not found'}, status_code=404)


@app.get('/search', response_model=List[Book])
async def search_books(title: str = '', author: str = '', min_price: float = 0, max_price: float = 1000):
    query = {}
    if title:
        query['title'] = {'$regex': f'{title}', '$options': 'i'}
    if author:
        query['author'] = {'$regex': f'{author}', '$options': 'i'}
    query['price'] = {'$gte': min_price, '$lte': max_price}
    books = collection.find(query)
    return [Book(**book) for book in books]


@app.get('/total_books')
async def total_books():
    total_books = collection.count_documents({})
    return JSONResponse(content={'total_books': total_books}, status_code=200)


@app.get('/top_books')
async def top_books():
    pipeline = [
        {'$group': {'_id': '$title', 'sales': {'$sum': '$items_sold'}}},
        {'$sort': {'sales': -1}},
        {'$limit': 5}
    ]
    books = collection.aggregate(pipeline)
    return JSONResponse(content=[book for book in books], status_code=200)


@app.get('/top_authors')
async def top_authors():
    pipeline = [
        {'$group': {'_id': '$author', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 5}
    ]
    authors = collection.aggregate(pipeline)
    return JSONResponse(content=[author for author in authors], status_code=200)


@app.put('/books/{book_id}/sell')
async def sell_book(book_id: str):
    book = collection.find_one({'_id': ObjectId(book_id)})
    if book:
        updated_book = {
            'items_sold': book['items_sold'] + 1,
            'stock': book['stock'] - 1
        }
        result = collection.update_one({'_id': ObjectId(book_id)}, {'$set': updated_book})
        if result.modified_count == 1:
            return JSONResponse(content={'message': 'Book sold successfully'}, status_code=200)
    return JSONResponse(content={'error': 'Book not found'}, status_code=404)


@app.put('/books/{book_id}/stock', response_model=Book)
async def update_book_stock(book_id: str, new_stock: int):
    try:
        book = collection.find_one({'_id': ObjectId(book_id)})
        if book is None:
            raise ValueError('Book not found')
        book['stock'] = new_stock
        result = collection.replace_one({'_id': ObjectId(book_id)}, book)
        if result.modified_count == 1:
            return Book(**book)
        else:
            raise ValueError('Error updating book stock')
    except Exception as e:
        return JSONResponse(content={'error': str(e)}, status_code=404)


# create the indexes for the MongoDB collection
collection.create_index([('title', 'text'), ('author', 'text')])
collection.create_index([('price', 1)])

# run the app
if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000)
