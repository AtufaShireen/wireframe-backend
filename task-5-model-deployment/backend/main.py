# backend/main.py

import uuid
import uvicorn
from fastapi import File
from fastapi import FastAPI
from fastapi import UploadFile
import numpy as np
from PIL import Image
import logging

from flask import Flask, render_template, url_for, request, redirect, session, flash
from functools import wraps

app = FastAPI()


def file_link(project_id,directory_path,blob_name):
    user_id = session['user_id']
    link = ''
    return link


def add_audio(user_id=None,query_id=None,):
    if not user_id:
        user_id = session['user_id']  
    else:
        return redirect(url_for("login"))
    
    return render_template('add_project.html')



def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kws):
            if ('email' in session) and ('user_id' in session):
                return f(*args, **kws) 
            else:
                flash("Thee shall not pass without login!")
                return redirect(url_for('login'))           
    return decorated_function

@app.get("/")
def homepage():
    return render_template('home.html')


@app.route("/login", methods=["POST", "GET"])
def login():
    if "email" in session:
        flash("Already logged in")
        return redirect(url_for("dashboard"))
    return render_template('login.html')

@app.route("/register", methods=['POST', 'GET'])
def register():
    if "email" in session:
        flash("Already registered")
        return redirect(url_for("dashboard"))

    return render_template('register.html')

@app.route("/userdashboard/", methods=['GET'])
@login_required
def dashboard(user_id=None):
    user_id = session['user_id']
    return render_template('dashboard.html')


@app.route("/input/", methods=['GET','POST'])
def input_page(user_id=None):
    user_id = session['user_id']
    return render_template('dashboard.html')





if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080)
