import pandas as pd
from flask import Flask,request,abort,jsonify,render_template
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
import datetime
from config import SECRET_KEY

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:utk%40123@localhost:5432/ecommerce-py'
app.config['SECRET_KEY'] = SECRET_KEY

db = SQLAlchemy(app)
migrate = Migrate(app, db)

class Users(db.Model):
    """
        Represents a user in the system.

        This class is a SQLAlchemy model for the 'users' table in the database.

        Attributes:
            user_id (int): The primary key, unique identifier for each user.
            username (str): The unique username for the user.
            password (str): The hashed password for the user. This field is required (cannot be null).
        """

    __tablename__ = 'users'
    user_id = db.Column(db.Integer,primary_key = True)
    username = db.Column(db.String(),unique = True)
    password = db.Column(db.String(),nullable = False)

class Product(db.Model):
    """
        Represents a product in the inventory.

        Attributes:
            product_id (int): The unique identifier for the product.
            product_name (str): The name of the product.
            category (str): The category to which the product belongs.
            price (float): The price of the product.
            quantity_sold (int): The number of units sold.
            rating (float): The average rating of the product.
            review_count (int): The number of reviews the product has received.
    """

    __tablename__ = 'products'
    product_id = db.Column(db.Integer, primary_key=True)
    product_name = db.Column(db.String(255), nullable=False)
    category = db.Column(db.String(255), nullable=False)
    price = db.Column(db.Numeric, nullable=False)
    quantity_sold = db.Column(db.Integer, nullable=False)
    rating = db.Column(db.Float, nullable=False)
    review_count = db.Column(db.Integer, nullable=False)

def create_summary(df):
    """

        This function transforms the input dataframe df to the required format for display which includes
        the following columns
        1)category
        2)total_revenue
        3)top_product - The most popular product in each category
        4)top_product_quantity_sold - The quantity sold of the top_product

        Input:
        df - It the input parameter which is a pandas dataframe with the data present that is to
        be transformed into the above-mentioned format

        Output:
        returns:
        final_summary - The pandas dataframe present in the required format


        """

    df['total_revenue'] = df['price']*df['quantity_sold']
    top_products = df.groupby('category')['quantity_sold'].max().reset_index()
    merged_df = pd.merge(df, top_products, on=['category', 'quantity_sold'])
    final_summary = merged_df[['category','total_revenue','product_name','quantity_sold']]
    final_summary = final_summary.rename(columns = {'product_name':'top_product','quantity_sold':'top_product_quantity_sold'})
    return final_summary

def upload_data():
    """

    This function uploads the data from a CSV file to database through SQLAlchemy. The data
    is preprocessed to remove null values before being uploaded to the database

    Input:
    product_data - It is not provided as a parameter but is read through a CSV file in the function

    Output:
    Returns:
        str: '200' to indicate success.
    Raises:
        400: If there is an error during the database transaction.

    """

    product_data = pd.read_csv('sample_data.csv')
    product_data['price'] = product_data['price'].fillna(product_data['price'].median())
    product_data['quantity_sold'] = product_data['quantity_sold'].fillna(product_data['quantity_sold'].median())
    product_data['rating'] = product_data['rating'].fillna(product_data['rating'].mean())
    try:
        for index, row in product_data.iterrows():
            product = Product(
                product_id=row['product_id'],
                product_name=row['product_name'],
                category=row['category'],
                price=row['price'],
                quantity_sold=row['quantity_sold'],
                rating=row['rating'],
                review_count=row['review_count']
            )
            db.session.add(product)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        print (e)
        abort(400)

    finally:
        return '200'

@app.route('/')
def index():
    """

        This function handles requests to the root URL of the Flask application ('/') and
        renders the 'index.html' template, passing the HTML table as a variable.

        Input:
        sample_data - sample_data is not passed as an input for the endpoint but is taken as
        an input from a CSV file in the directory

        Output
        returns:
             Rendered HTML of the 'index.html' template with the processed data table.

        """
    upload_status = upload_data()
    if (upload_status=='200'):
        print ("upload success")
    sample_data = pd.read_csv("sample_data.csv")
    df = create_summary(sample_data)
    df_html = df.to_html(classes='table', index=False)
    return render_template('index.html', table=df_html)

@app.route('/signup',methods= ['POST'])
def add_user():

    """
        This endpoint handles user sign-up by adding a new user to the database using a post request.

        Input:
        request_info - The json object present in the request that contains the username and
        password that is to be uploaded to the database

        Returns:
            str: '200' to indicate that the user was successfully added.

        Raises:
            400: If there is an error during the database transaction.
    """

    request_info = request.get_json()
    request_info['password'] = generate_password_hash(request_info['password'])
    user_info = Users(username = request_info.get('username'),password = request_info.get('password'))
    try:
        db.session.add(user_info)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        print (e)
        abort(400)
    finally:
        return '200'

@app.route('/login',methods = ['POST'])
def verify_user():

    """
       This function authenticates a user and generates a JWT token upon successful login.

       Input:
        user_info - The json object present in the request that contains the username and
        password that is to be authenticated from the database

       Returns:
           response: A JSON response containing the JWT token if authentication is successful,
                     or an error message with a 401 status code if authentication fails.

       Raises:
           401: If authentication fails or there is an error during the authentication process.
       """

    user_info = request.get_json()
    username = user_info.get('username')
    password = user_info.get('password')
    try:
        user = Users.query.filter_by(username=username).first()
        if (user,check_password_hash(user.password,password)):
                token = jwt.encode({
                    'username': user.username,
                    'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=1)
                }, app.config['SECRET_KEY'], algorithm='HS256')

                return jsonify({'token': token})
        else:
            return jsonify({"error":'Authentication Failed'})
    except Exception as e:
        print (e)
        abort(401)
    finally:
        return '200'


if __name__ == "__main__":
    app.run(debug=True)
