\
from flask import Flask, redirect, url_for
from flask_login import LoginManager

from models.user import load_user
from blueprints.auth import auth_bp
from blueprints.admin_rrhh import admin_rrhh_bp
from blueprints.modulos import modulos_bp

def create_app():
    app = Flask(__name__)
    app.secret_key = "dev-secret-change-me"

    login_manager = LoginManager()
    login_manager.login_view = "auth.login"
    login_manager.init_app(app)

    @login_manager.user_loader
    def _load(user_id):
        return load_user(user_id)

    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_rrhh_bp)
    app.register_blueprint(modulos_bp)

    @app.route("/")
    def index():
        return redirect(url_for("modulos.dashboard"))

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host="127.0.0.1", port=5011, debug=True)
