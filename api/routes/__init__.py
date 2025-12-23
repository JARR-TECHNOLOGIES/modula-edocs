import pkgutil
import importlib
from flask import Flask
from flask_smorest import Api
from extensions.logging import get_logger

logger = get_logger(__name__, class_name="RoutesInitializer")
from routes import __path__ as ROUTES_PATH


def init_routes(app: Flask):
    api = Api(app)

    logger.debug("Initializing route blueprints (auto-discovery)")

    for module_info in pkgutil.iter_modules(ROUTES_PATH):
        module_name = module_info.name

        # Ignore folders such as "schemas"
        if module_name.startswith("schemas") or module_name.startswith("_"):
            continue

        full_path = f"routes.{module_name}"
        logger.debug(f"Importing route module {full_path}")

        try:
            module = importlib.import_module(full_path)

            # The ONLY thing we expect inside is `blp`
            blp = getattr(module, "blp", None)

            if blp is None:
                logger.debug(f"No 'blp' found in {full_path}, skipping module")
                continue

            api.register_blueprint(blp)
            logger.info(f"Registered blueprint '{blp.name}' from {full_path}")

        except Exception as exc:
            logger.error(f"Failed importing {full_path}: {exc}", exc_info=True)

    return app