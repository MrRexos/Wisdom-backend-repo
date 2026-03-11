# Reglas del Proyecto
- Encoding: UTF-8 obligatorio.
- No corromper caracteres especiales en comentarios y strings en español,chino,arabe,catalan,ingles,frances o el resto de idiomas.



# Contexto de la Base de Datos

> **Nota para el LLM:** Este documento describe la estructura exacta de la base de datos MySQL (tablas, columnas, tipos de datos, restricciones y relaciones) para asegurar una perfecta alineación entre el frontend, el backend y la persistencia de datos. Siempre que generes o modifiques una APi o funcionalidad respeta estrictamente la nulabilidad, los valores por defecto y las relaciones listadas aquí. Si crees que se ha de modificar o añadir una tabla siempre comunicamenlo i actualiza este documento

## Tabla: `address`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `address_type` (VARCHAR(100), NULL)
- `street_number` (INT, NOT NULL)
- `address_1` (VARCHAR(255), NOT NULL)
- `address_2` (VARCHAR(255), NULL)
- `postal_code` (VARCHAR(10), NOT NULL)
- `city` (VARCHAR(100), NOT NULL)
- `state` (VARCHAR(100), NOT NULL)
- `country` (VARCHAR(100), NOT NULL)
- `latitude` (DECIMAL(10,8), NULL)
- `longitude` (DECIMAL(10,8), NULL)

## Tabla: `auth_session`
- `id` (BIGINT UNSIGNED, PK, auto_increment, NOT NULL)
- `user_id` (BIGINT UNSIGNED, NOT NULL)
- `refresh_token_hash` (CHAR(64), NOT NULL)
- `user_agent` (VARCHAR(255), NULL)
- `ip` (VARCHAR(45), NULL)
- `created_at` (DATETIME, DEFAULT_GENERATED, NOT NULL, DEFAULT CURRENT_TIMESTAMP)
- `last_used_at` (DATETIME, NULL)
- `expires_at` (DATETIME, NOT NULL)
- `revoked_at` (DATETIME, NULL)

## Tabla: `booking`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `user_id` (INT UNSIGNED, FK -> user_account.id, NULL)
- `service_id` (INT, FK -> service.id, NULL)
- `address_id` (INT, FK -> address.id, NULL)
- `payment_method_id` (INT, FK -> payment_method.id, NULL)
- `booking_start_datetime` (DATETIME, NULL)
- `booking_end_datetime` (DATETIME, NULL)
- `recurrent_pattern_id` (INT, FK -> recurrent_pattern.id, NULL)
- `promotion_id` (INT, FK -> promotion.id, NULL)
- `service_duration` (INT, NULL)
- `final_price` (DECIMAL(9,2), NULL)
- `commission` (DECIMAL(9,2), NULL)
- `is_paid` (TINYINT(1), NOT NULL, DEFAULT 0)
- `booking_status` (ENUM('PENDING_DEPOSIT','REQUESTED','ACCEPTED','REJECTED','CANCELED','COMPLETED','PAYMENT_FAILED'), NOT NULL, DEFAULT pending_deposit)
- `order_datetime` (DATETIME, NOT NULL)
- `description` (TEXT, NULL)

## Tabla: `collection_method`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `user_id` (INT UNSIGNED, FK -> user_account.id, NOT NULL)
- `type` (ENUM('IBAN','CARD'), NOT NULL, DEFAULT iban)
- `provider` (ENUM('STRIPE'), NOT NULL, DEFAULT stripe)
- `external_account_id` (VARCHAR(64), NOT NULL)
- `last4` (VARCHAR(4), NOT NULL)
- `brand` (VARCHAR(32), NULL)
- `currency` (CHAR(3), NOT NULL, DEFAULT EUR)
- `is_default` (TINYINT, NOT NULL, DEFAULT 1)
- `created_at` (DATETIME, DEFAULT_GENERATED, NULL, DEFAULT CURRENT_TIMESTAMP)
- `updated_at` (DATETIME, DEFAULT_GENERATED on update CURRENT_TIMESTAMP, NULL, DEFAULT CURRENT_TIMESTAMP)
- `address_id` (INT UNSIGNED, NOT NULL)
- `full_name` (VARCHAR(200), NOT NULL)

## Tabla: `consult`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `address_id` (INT, FK -> address.id, NULL)
- `service_id` (INT, FK -> service.id, NULL)
- `user_id` (INT UNSIGNED, FK -> user_account.id, NULL)
- `consult_duration` (TIME, NOT NULL, DEFAULT 00:15:00)
- `small_description` (TEXT, NULL)
- `consult_start_datetime` (DATETIME, NOT NULL)

## Tabla: `consult_via`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `provider` (VARCHAR(45), NOT NULL)
- `username` (VARCHAR(45), NULL)
- `url` (VARCHAR(45), NULL)

## Tabla: `directions`
- `id` (INT, PK, auto_increment, NOT NULL)
- `user_id` (INT UNSIGNED, FK -> user_account.id, NOT NULL)
- `address_id` (INT, FK -> address.id, NOT NULL)

## Tabla: `experience_place`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `service_id` (INT, FK -> service.id, NULL)
- `experience_title` (VARCHAR(255), NOT NULL)
- `place_name` (VARCHAR(255), NULL)
- `experience_started_date` (DATE, NOT NULL)
- `experience_end_date` (DATE, NULL)

## Tabla: `item_list`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `list_id` (INT, FK -> service_list.id, NULL)
- `service_id` (INT, FK -> service.id, NULL)
- `note` (TEXT, NULL)
- `order` (INT, NOT NULL)
- `added_datetime` (DATETIME, NOT NULL)

## Tabla: `password_reset_codes`
- `user_id` (INT UNSIGNED, PK, UNIQUE, FK -> user_account.id, NOT NULL)
- `code` (VARCHAR(10), NOT NULL)
- `expires_at` (TIMESTAMP, NOT NULL)

## Tabla: `payment_method`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `user_id` (INT UNSIGNED, FK -> user_account.id, NULL)
- `payment_type` (VARCHAR(100), NOT NULL)
- `provider` (VARCHAR(100), NULL)
- `card_number` (VARCHAR(25), NOT NULL)
- `expiry_date` (CHAR(5), NOT NULL)
- `is_safed` (TINYINT(1), NOT NULL, DEFAULT 0)
- `is_default` (TINYINT(1), NOT NULL, DEFAULT 0)

## Tabla: `payments`
- `id` (INT UNSIGNED, PK, auto_increment, NOT NULL)
- `booking_id` (INT, FK -> booking.id, NULL)
- `type` (VARCHAR(100), NOT NULL)
- `payment_intent_id` (VARCHAR(250), NULL)
- `amount_cents` (INT, NULL)
- `commission_snapshot_cents` (INT, NULL)
- `final_price_snapshot_cents` (INT, NULL)
- `status` (VARCHAR(100), NOT NULL)
- `currency` (VARCHAR(3), NOT NULL, DEFAULT eur)
- `payment_method_last4` (VARCHAR(4), NULL)
- `last_error_code` (VARCHAR(64), NULL)
- `last_error_message` (VARCHAR(255), NULL)
- `created_at` (TIMESTAMP, DEFAULT_GENERATED, NOT NULL, DEFAULT CURRENT_TIMESTAMP)
- `updated_at` (TIMESTAMP, DEFAULT_GENERATED on update CURRENT_TIMESTAMP, NOT NULL, DEFAULT CURRENT_TIMESTAMP)
- `payment_method_id` (VARCHAR(64), NULL)
- `transfer_group` (VARCHAR(64), NULL)

## Tabla: `price`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `price` (DECIMAL(9,2), NULL)
- `price_type` (VARCHAR(100), NOT NULL)

## Tabla: `promotion`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `promotion_tittle` (VARCHAR(255), NOT NULL)
- `promotion_code` (VARCHAR(255), NOT NULL)
- `discount_rate` (INT, NOT NULL)
- `promotion_start_date` (DATE, NOT NULL)
- `promotion_end_date` (DATE, NULL)

## Tabla: `recurrent_event_exception`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `booking_id` (INT, FK -> booking.id, NOT NULL)
- `is_rescheduled` (TINYINT(1), NOT NULL)
- `is_cancelled` (TINYINT(1), NOT NULL)
- `exception_start_datetime` (DATETIME, NOT NULL)
- `exception_end_datetime` (DATETIME, NOT NULL)
- `exception_created_datetime` (DATETIME, NOT NULL)
- `recurrent_pattern_id` (INT, FK -> recurrent_pattern.id, NULL)

## Tabla: `recurrent_pattern`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `pattern_type` (VARCHAR(100), NOT NULL)
- `separation` (INT, NOT NULL)
- `max_num_of_occurrencies` (INT, NULL)
- `day_of_week` (INT, NULL)
- `week_of_month` (INT, NULL)
- `day_of_month` (INT, NULL)
- `month_of_year` (INT, NULL)
- `is_active` (TINYINT(1), NOT NULL, DEFAULT 1)

## Tabla: `review`
- `id` (INT, PK, auto_increment, NOT NULL)
- `user_id` (INT UNSIGNED, FK -> user_account.id, NOT NULL)
- `service_id` (INT, FK -> service.id, NOT NULL)
- `rating` (DECIMAL(2,1), NOT NULL)
- `comment` (TEXT, NULL)
- `review_datetime` (DATETIME, NOT NULL)

## Tabla: `service`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `service_title` (VARCHAR(255), NOT NULL)
- `user_id` (INT UNSIGNED, FK -> user_account.id, NOT NULL)
- `description` (TEXT, NULL)
- `service_category_id` (INT, FK -> service_category.id, NULL)
- `price_id` (INT, FK -> price.id, NULL)
- `latitude` (DECIMAL(10,8), NULL)
- `longitude` (DECIMAL(11,8), NULL)
- `action_rate` (INT, NULL)
- `user_can_ask` (TINYINT(1), NOT NULL)
- `user_can_consult` (TINYINT(1), NOT NULL)
- `price_consult` (DECIMAL(8,2), NULL)
- `consult_via_id` (INT, FK -> consult_via.id, NULL)
- `is_individual` (TINYINT(1), NOT NULL)
- `allow_discounts` (TINYINT(1), NOT NULL, DEFAULT 1)
- `discount_rate` (INT, NULL, DEFAULT 10)
- `hobbies` (TEXT, NULL)
- `is_hidden` (TINYINT, NOT NULL, DEFAULT 0)
- `last_edit_datetime` (DATETIME, NULL)
- `service_created_datetime` (DATETIME, NOT NULL)

## Tabla: `service_category`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `service_category_type_id` (INT, FK -> service_category_type.id, NULL)
- `service_family_id` (INT, FK -> service_family.id, NULL)

## Tabla: `service_category_type`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `service_category_name` (VARCHAR(255), NOT NULL)
- `description` (TEXT, NULL)

## Tabla: `service_family`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `service_family` (VARCHAR(255), NOT NULL)
- `description` (TEXT, NULL)

## Tabla: `service_image`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `service_id` (INT, FK -> service.id, NOT NULL)
- `image_url` (VARCHAR(255), NOT NULL)
- `order` (INT, NOT NULL)
- `object_name` (VARCHAR(512), NULL)

## Tabla: `service_language`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `service_id` (INT, FK -> service.id, NULL)
- `language` (CHAR(2), NOT NULL)

## Tabla: `service_list`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `list_name` (VARCHAR(255), NOT NULL)
- `user_id` (INT UNSIGNED, FK -> user_account.id, NOT NULL)

## Tabla: `service_report`
- `id` (INT, PK, auto_increment, NOT NULL)
- `service_id` (INT, FK -> service.id, NOT NULL)
- `reporter_user_id` (INT UNSIGNED, FK -> user_account.id, NOT NULL)
- `reason_code` (ENUM('FRAUD','SPAM','INCORRECT_INFO','PRICING_ISSUE','EXTERNAL_CONTACT','INAPPROPRIATE','DUPLICATE','OTHER'), NOT NULL)
- `reason_text` (VARCHAR(255), NULL)
- `description` (TEXT, NULL)
- `status` (ENUM('PENDING','IN_REVIEW','RESOLVED','DISMISSED'), NOT NULL, DEFAULT pending)
- `report_datetime` (DATETIME, DEFAULT_GENERATED, NOT NULL, DEFAULT CURRENT_TIMESTAMP)
- `handled_by_user_id` (INT UNSIGNED, FK -> user_account.id, NULL)
- `handled_datetime` (DATETIME, NULL)
- `resolution_notes` (TEXT, NULL)

## Tabla: `service_report_attachment`
- `id` (INT, PK, auto_increment, NOT NULL)
- `report_id` (INT, FK -> service_report.id, NOT NULL)
- `file_url` (VARCHAR(500), NOT NULL)
- `file_type` (VARCHAR(50), NOT NULL)
- `created_datetime` (DATETIME, DEFAULT_GENERATED, NOT NULL, DEFAULT CURRENT_TIMESTAMP)

## Tabla: `service_tags`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `service_id` (INT, FK -> service.id, NOT NULL)
- `tag` (VARCHAR(255), NOT NULL)

## Tabla: `shared_list`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `list_id` (INT, FK -> service_list.id, NOT NULL)
- `user_id` (INT UNSIGNED, FK -> user_account.id, NOT NULL)
- `permissions` (VARCHAR(45), NOT NULL)

## Tabla: `user_account`
- `id` (INT UNSIGNED, PK, UNIQUE, auto_increment, NOT NULL)
- `email` (VARCHAR(255), UNIQUE, NOT NULL)
- `username` (VARCHAR(100), UNIQUE, NOT NULL)
- `password` (VARCHAR(60), NULL)
- `first_name` (VARCHAR(100), NOT NULL)
- `surname` (VARCHAR(100), NOT NULL)
- `profile_picture` (VARCHAR(255), NULL)
- `is_professional` (TINYINT(1), NOT NULL, DEFAULT 0)
- `platform` (ENUM('ios', 'android'), NOT NULL, DEFAULT 'ios'): Indica plataforma del dispositivo
- `auth_provider` (ENUM('email', 'google', 'apple'), NOT NULL, DEFAULT 'email'): Indica el método de autenticación.
- `provider_id` (VARCHAR(255), UNIQUE, NULL): Almacena el ID único de Google/Apple si aplica.
- `language` (VARCHAR(2), NOT NULL, DEFAULT en)
- `allow_notis` (TINYINT(1), NULL, DEFAULT 1)
- `currency` (CHAR(3), NOT NULL, DEFAULT EUR)
- `money_in_wallet` (DECIMAL(10,2) UNSIGNED, NOT NULL, DEFAULT 0.00)
- `professional_started_datetime` (DATETIME, NULL)
- `is_expert` (TINYINT(1), NOT NULL, DEFAULT 0)
- `is_verified` (TINYINT(1), NOT NULL, DEFAULT 0)
- `strikes_num` (TINYINT(1), NOT NULL, DEFAULT 0)
- `date_of_birth` (DATE, NULL)
- `nif` (VARCHAR(32), NULL)
- `stripe_account_id` (VARCHAR(64), NULL)
- `stripe_customer_id` (VARCHAR(64), NULL)
- `phone` (CHAR(16), NULL)
- `vacation_mode` (TINYINT(1), NOT NULL, DEFAULT 0)
- `joined_datetime` (DATETIME, NOT NULL)

## Tabla: `user_address`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `user_id` (INT UNSIGNED, FK -> user_account.id, NOT NULL)
- `address_id` (INT, FK -> address.id, NOT NULL)
- `is_default` (TINYINT(1), NOT NULL, DEFAULT 0)

## Tabla: `user_availability`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `user_id` (INT UNSIGNED, FK -> user_account.id, NOT NULL)
- `available_event_start_datetime` (DATETIME, NOT NULL)
- `available_event_end_datetime` (DATETIME, NOT NULL)

## Tabla: `user_not_available`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `user_id` (INT UNSIGNED, FK -> user_account.id, NOT NULL)
- `not_available_event_start_datetime` (DATETIME, NOT NULL)
- `not_available_event_end_datetime` (DATETIME, NOT NULL)