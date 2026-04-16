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
- `client_user_id` (INT UNSIGNED, FK -> user_account.id, NULL)
- `service_id` (INT, FK -> service.id, NULL)
- `provider_user_id_snapshot` (INT UNSIGNED, FK -> user_account.id, NULL)
- `address_id` (INT, FK -> address.id, NULL)
- `description` (TEXT, NULL)
- `service_status` (ENUM('pending_deposit','requested','accepted','in_progress','finished','canceled','expired'), NOT NULL, DEFAULT pending_deposit)
- `settlement_status` (ENUM('none','pending_client_approval','awaiting_payment','paid','refund_pending','partially_refunded','refunded','payment_failed','manual_review_required','in_dispute'), NOT NULL, DEFAULT none)
- `order_datetime` (DATETIME, NOT NULL, DEFAULT CURRENT_TIMESTAMP)
- `updated_at` (DATETIME, DEFAULT_GENERATED on update CURRENT_TIMESTAMP, NOT NULL, DEFAULT CURRENT_TIMESTAMP)
- `requested_start_datetime` (DATETIME, NULL)
- `requested_duration_minutes` (INT, NULL)
- `requested_end_datetime` (DATETIME, NULL)
- `deposit_confirmed_at` (DATETIME, NULL)
- `accepted_at` (DATETIME, NULL)
- `started_at` (DATETIME, NULL)
- `finished_at` (DATETIME, NULL)
- `canceled_at` (DATETIME, NULL)
- `expired_at` (DATETIME, NULL)
- `accept_deadline_at` (DATETIME, NULL)
- `expires_at` (DATETIME, NOT NULL)
- `client_approval_deadline_at` (DATETIME, NULL)
- `last_minute_window_starts_at` (DATETIME, NULL)
- `canceled_by_user_id` (INT UNSIGNED, FK -> user_account.id, NULL)
- `cancellation_reason_code` (VARCHAR(100), NULL)
- `cancellation_note` (TEXT, NULL)
- `service_title_snapshot` (VARCHAR(255), NULL)
- `price_type_snapshot` (VARCHAR(100), NULL)
- `service_currency_snapshot` (CHAR(3), NOT NULL, DEFAULT EUR)
- `unit_price_amount_cents_snapshot` (INT, NULL)
- `minimum_notice_policy_snapshot` (INT, NULL)
- `estimated_base_amount_cents` (INT, NULL)
- `estimated_commission_amount_cents` (INT, NULL)
- `estimated_total_amount_cents` (INT, NULL)
- `selected_customer_payment_method_id` (INT, FK -> payment_method.id, NULL)
- `deposit_amount_cents_snapshot` (INT, NULL)
- `deposit_currency_snapshot` (CHAR(3), NULL)
- `updated_at` (DATETIME, NOT NULL, DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)

## Tabla: `booking_change_request`
- `id` (INT UNSIGNED, PK, UNIQUE, auto_increment, NOT NULL)
- `booking_id` (INT, FK -> booking.id, NOT NULL)
- `requested_by_user_id` (INT UNSIGNED, FK -> user_account.id, NOT NULL)
- `target_user_id` (INT UNSIGNED, FK -> user_account.id, NOT NULL)
- `status` (ENUM('pending','accepted','rejected','canceled','expired'), NOT NULL, DEFAULT pending)
- `changes_json` (JSON, NOT NULL)
- `message` (TEXT, NULL)
- `created_at` (DATETIME, DEFAULT_GENERATED, NOT NULL, DEFAULT CURRENT_TIMESTAMP)
- `resolved_at` (DATETIME, NULL)

## Tabla: `booking_closure_proposal`
- `id` (INT UNSIGNED, PK, UNIQUE, auto_increment, NOT NULL)
- `booking_id` (INT, FK -> booking.id, NOT NULL)
- `created_by_user_id` (INT UNSIGNED, FK -> user_account.id, NOT NULL)
- `status` (ENUM('active','revoked','accepted','rejected','superseded'), NOT NULL, DEFAULT active)
- `price_type_snapshot` (VARCHAR(100), NOT NULL)
- `estimated_duration_minutes` (INT, NULL)
- `proposed_final_duration_minutes` (INT, NULL)
- `estimated_total_amount_cents` (INT, NOT NULL)
- `proposed_base_amount_cents` (INT, NOT NULL)
- `proposed_commission_amount_cents` (INT, NOT NULL)
- `proposed_total_amount_cents` (INT, NOT NULL)
- `deposit_already_paid_amount_cents` (INT, NOT NULL, DEFAULT 0)
- `amount_due_from_client_cents` (INT, NOT NULL)
- `amount_to_refund_cents` (INT, NOT NULL, DEFAULT 0)
- `provider_payout_amount_cents` (INT, NOT NULL)
- `platform_amount_cents` (INT, NOT NULL)
- `zero_charge_mode` (TINYINT(1), NOT NULL, DEFAULT 0)
- `auto_charge_eligible` (TINYINT(1), NOT NULL, DEFAULT 0)
- `auto_charge_scheduled_at` (DATETIME, NULL)
- `sent_at` (DATETIME, DEFAULT_GENERATED, NOT NULL, DEFAULT CURRENT_TIMESTAMP)
- `revoked_at` (DATETIME, NULL)
- `accepted_at` (DATETIME, NULL)
- `rejected_at` (DATETIME, NULL)

## Tabla: `booking_issue_report`
- `id` (INT UNSIGNED, PK, UNIQUE, auto_increment, NOT NULL)
- `booking_id` (INT, FK -> booking.id, NOT NULL)
- `reported_by_user_id` (INT UNSIGNED, FK -> user_account.id, NOT NULL)
- `reported_against_user_id` (INT UNSIGNED, FK -> user_account.id, NULL)
- `issue_type` (ENUM('no_show_client','no_show_provider','last_minute_client','last_minute_provider','general_problem','payment_dispute'), NOT NULL)
- `status` (ENUM('open','resolved','dismissed'), NOT NULL, DEFAULT open)
- `details` (TEXT, NOT NULL)
- `created_at` (DATETIME, DEFAULT_GENERATED, NOT NULL, DEFAULT CURRENT_TIMESTAMP)
- `resolved_at` (DATETIME, NULL)

## Tabla: `booking_status_history`
- `id` (INT UNSIGNED, PK, UNIQUE, auto_increment, NOT NULL)
- `booking_id` (INT, FK -> booking.id, NOT NULL)
- `from_service_status` (VARCHAR(50), NULL)
- `to_service_status` (VARCHAR(50), NOT NULL)
- `from_settlement_status` (VARCHAR(50), NULL)
- `to_settlement_status` (VARCHAR(50), NOT NULL)
- `changed_by_user_id` (INT UNSIGNED, FK -> user_account.id, NULL)
- `reason_code` (VARCHAR(100), NULL)
- `note` (TEXT, NULL)
- `created_at` (DATETIME, DEFAULT_GENERATED, NOT NULL, DEFAULT CURRENT_TIMESTAMP)

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
- `brand` (VARCHAR(50), NULL)
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
- `provider_payout_amount_cents` (INT, NULL)
- `provider_payout_status` (ENUM('none','pending_release','released','partially_reversed','reversed'), NOT NULL, DEFAULT none)
- `provider_payout_eligible_at` (DATETIME, NULL)
- `provider_payout_released_at` (DATETIME, NULL)
- `provider_payout_transfer_id` (VARCHAR(64), NULL)
- `created_at` (TIMESTAMP, DEFAULT_GENERATED, NOT NULL, DEFAULT CURRENT_TIMESTAMP)
- `updated_at` (TIMESTAMP, DEFAULT_GENERATED on update CURRENT_TIMESTAMP, NOT NULL, DEFAULT CURRENT_TIMESTAMP)
- `payment_method_id` (VARCHAR(64), NULL)
- `transfer_group` (VARCHAR(64), NULL)

## Tabla: `price`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `price` (DECIMAL(9,2), NULL)
- `currency` (CHAR(3), NOT NULL, DEFAULT EUR)
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
- `experience_years` (INT, NOT NULL, DEFAULLT 1)
- `user_can_ask` (TINYINT(1), NOT NULL)
- `user_can_consult` (TINYINT(1), NOT NULL)
- `price_consult` (DECIMAL(8,2), NULL)
- `consult_via_id` (INT, FK -> consult_via.id, NULL)
- `is_individual` (TINYINT(1), NOT NULL)
- `minimum_notice_policy` (INT, NULL)
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
- `category_key` (VARCHAR(255), NOT NULL)

## Tabla: `service_family`
- `id` (INT, PK, UNIQUE, auto_increment, NOT NULL)
- `family_key` (VARCHAR(255), NOT NULL)

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
- `country` (CHAR(2), DEFAULT NULL)
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

## Tabla: `user_strike`
- `id` (INT UNSIGNED, PK, UNIQUE, auto_increment, NOT NULL)
- `user_id` (INT UNSIGNED, FK -> user_account.id, NOT NULL)
- `booking_id` (INT, FK -> booking.id, NULL)
- `issue_report_id` (INT UNSIGNED, FK -> booking_issue_report.id, NULL)
- `reason_code` (VARCHAR(100), NOT NULL)
- `status` (ENUM('active','forgiven'), NOT NULL, DEFAULT active)
- `created_at` (DATETIME, DEFAULT_GENERATED, NOT NULL, DEFAULT CURRENT_TIMESTAMP)
- `forgiven_at` (DATETIME, NULL)


----------------------------------------------------------


## Relaciones y Keys (PK/FK)

### Tabla: `address`
- `id`: **PK** (Key Name: `PRIMARY`)

### Tabla: `auth_session`
- `id`: **PK** (Key Name: `PRIMARY`)

### Tabla: `booking`
- `id`: **PK** (Key Name: `PRIMARY`)
- `client_user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_booking_client_user_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)
- `provider_user_id_snapshot`: **FK** -> `user_account`.`id` (Key Name: `fk_booking_provider_user_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)
- `address_id`: **FK** -> `address`.`id` (Key Name: `fk_booking_address_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)
- `service_id`: **FK** -> `service`.`id` (Key Name: `fk_booking_service_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)
- `selected_customer_payment_method_id`: **FK** -> `payment_method`.`id` (Key Name: `fk_booking_payment_method_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)
- `canceled_by_user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_booking_canceled_by_user_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)

### Tabla: `booking_change_request`
- `id`: **PK** (Key Name: `PRIMARY`)
- `booking_id`: **FK** -> `booking`.`id` (Key Name: `fk_bcr_booking`, ON DELETE CASCADE, ON UPDATE NO ACTION)
- `requested_by_user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_bcr_req_user`, ON DELETE CASCADE, ON UPDATE NO ACTION)
- `target_user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_bcr_tgt_user`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `booking_closure_proposal`
- `id`: **PK** (Key Name: `PRIMARY`)
- `booking_id`: **FK** -> `booking`.`id` (Key Name: `fk_bcp_booking`, ON DELETE CASCADE, ON UPDATE NO ACTION)
- `created_by_user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_bcp_user`, ON DELETE RESTRICT, ON UPDATE NO ACTION)

### Tabla: `booking_issue_report`
- `id`: **PK** (Key Name: `PRIMARY`)
- `booking_id`: **FK** -> `booking`.`id` (Key Name: `fk_bir_booking`, ON DELETE CASCADE, ON UPDATE NO ACTION)
- `reported_by_user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_bir_rep_user`, ON DELETE CASCADE, ON UPDATE NO ACTION)
- `reported_against_user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_bir_tgt_user`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `booking_status_history`
- `id`: **PK** (Key Name: `PRIMARY`)
- `booking_id`: **FK** -> `booking`.`id` (Key Name: `fk_bsh_booking`, ON DELETE CASCADE, ON UPDATE NO ACTION)
- `changed_by_user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_bsh_user`, ON DELETE SET NULL, ON UPDATE NO ACTION)

### Tabla: `collection_method`
- `id`: **PK** (Key Name: `PRIMARY`)
- `user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_collection_method_user_id_user_account_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `consult`
- `id`: **PK** (Key Name: `PRIMARY`)
- `address_id`: **FK** -> `address`.`id` (Key Name: `fk_consult_address_id_address_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)
- `service_id`: **FK** -> `service`.`id` (Key Name: `fk_consult_service_id_service_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)
- `user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_consult_user_id_user_account_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)

### Tabla: `consult_via`
- `id`: **PK** (Key Name: `PRIMARY`)

### Tabla: `directions`
- `id`: **PK** (Key Name: `PRIMARY`)
- `address_id`: **FK** -> `address`.`id` (Key Name: `fk_directions_address_id_address_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)
- `user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_directions_user_id_user_account_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `experience_place`
- `id`: **PK** (Key Name: `PRIMARY`)
- `service_id`: **FK** -> `service`.`id` (Key Name: `fk_experience_place_service_id_service_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)

### Tabla: `item_list`
- `id`: **PK** (Key Name: `PRIMARY`)
- `list_id`: **FK** -> `service_list`.`id` (Key Name: `fk_item_list_list_id_service_list_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)
- `service_id`: **FK** -> `service`.`id` (Key Name: `fk_item_list_service_id_service_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `password_reset_codes`
- `user_id`: **PK** (Key Name: `PRIMARY`)
- `user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_user_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `payment_method`
- `id`: **PK** (Key Name: `PRIMARY`)
- `user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_payment_method_user_id_user_account_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)

### Tabla: `payments`
- `id`: **PK** (Key Name: `PRIMARY`)
- `booking_id`: **FK** -> `booking`.`id` (Key Name: `fk_payments_booking_id_booking_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)

### Tabla: `price`
- `id`: **PK** (Key Name: `PRIMARY`)

### Tabla: `promotion`
- `id`: **PK** (Key Name: `PRIMARY`)

### Tabla: `recurrent_event_exception`
- `id`: **PK** (Key Name: `PRIMARY`)
- `booking_id`: **FK** -> `booking`.`id` (Key Name: `fk_recurrent_event_exception_booking_id_booking_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)
- `recurrent_pattern_id`: **FK** -> `recurrent_pattern`.`id` (Key Name: `fk_recurrent_event_exception_recurrent_pattern_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `recurrent_pattern`
- `id`: **PK** (Key Name: `PRIMARY`)

### Tabla: `review`
- `id`: **PK** (Key Name: `PRIMARY`)
- `service_id`: **FK** -> `service`.`id` (Key Name: `fk_review_service_id_service_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)
- `user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_review_user_id_user_account_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `service`
- `id`: **PK** (Key Name: `PRIMARY`)
- `consult_via_id`: **FK** -> `consult_via`.`id` (Key Name: `fk_service_consult_via_id_consult_via_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)
- `price_id`: **FK** -> `price`.`id` (Key Name: `fk_service_price_id_price_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)
- `service_category_id`: **FK** -> `service_category`.`id` (Key Name: `fk_service_service_category_id_service_category_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)
- `user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_service_user_id_user_account_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `service_category`
- `id`: **PK** (Key Name: `PRIMARY`)
- `service_category_type_id`: **FK** -> `service_category_type`.`id` (Key Name: `fk_service_category_service_category_type_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)
- `service_family_id`: **FK** -> `service_family`.`id` (Key Name: `fk_service_category_service_family_id_service_family_id`, ON DELETE SET NULL, ON UPDATE NO ACTION)

### Tabla: `service_category_type`
- `id`: **PK** (Key Name: `PRIMARY`)

### Tabla: `service_family`
- `id`: **PK** (Key Name: `PRIMARY`)

### Tabla: `service_image`
- `id`: **PK** (Key Name: `PRIMARY`)
- `service_id`: **FK** -> `service`.`id` (Key Name: `fk_service_image_service_id_service_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `service_language`
- `id`: **PK** (Key Name: `PRIMARY`)
- `service_id`: **FK** -> `service`.`id` (Key Name: `fk_service_language_service_id_service_id`, ON DELETE CASCADE, ON UPDATE RESTRICT)

### Tabla: `service_list`
- `id`: **PK** (Key Name: `PRIMARY`)
- `user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_service_list_user_id_user_account_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `service_report`
- `id`: **PK** (Key Name: `PRIMARY`)
- `handled_by_user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_sr_handled_by`, ON DELETE SET NULL, ON UPDATE NO ACTION)
- `service_id`: **FK** -> `service`.`id` (Key Name: `fk_sr_service`, ON DELETE CASCADE, ON UPDATE NO ACTION)
- `reporter_user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_sr_reporter`, ON DELETE RESTRICT, ON UPDATE NO ACTION)

### Tabla: `service_report_attachment`
- `id`: **PK** (Key Name: `PRIMARY`)
- `report_id`: **FK** -> `service_report`.`id` (Key Name: `fk_sra_report`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `service_tags`
- `id`: **PK** (Key Name: `PRIMARY`)
- `service_id`: **FK** -> `service`.`id` (Key Name: `fk_service_tags_service_id_service_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `shared_list`
- `id`: **PK** (Key Name: `PRIMARY`)
- `list_id`: **FK** -> `service_list`.`id` (Key Name: `fk_shared_list_list_id_service_list_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)
- `user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_shared_list_user_id_user_account_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `user_account`
- `id`: **PK** (Key Name: `PRIMARY`)

### Tabla: `user_address`
- `id`: **PK** (Key Name: `PRIMARY`)
- `address_id`: **FK** -> `address`.`id` (Key Name: `fk_user_address_address_id_address_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)
- `user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_user_address_user_id_user_account_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `user_availability`
- `id`: **PK** (Key Name: `PRIMARY`)
- `user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_user_availability_user_id_user_account_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `user_not_available`
- `id`: **PK** (Key Name: `PRIMARY`)
- `user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_user_not_available_user_id_user_account_id`, ON DELETE CASCADE, ON UPDATE NO ACTION)

### Tabla: `user_strike`
- `id`: **PK** (Key Name: `PRIMARY`)
- `user_id`: **FK** -> `user_account`.`id` (Key Name: `fk_us_user`, ON DELETE CASCADE, ON UPDATE NO ACTION)
- `booking_id`: **FK** -> `booking`.`id` (Key Name: `fk_us_booking`, ON DELETE SET NULL, ON UPDATE NO ACTION)
- `issue_report_id`: **FK** -> `booking_issue_report`.`id` (Key Name: `fk_us_issue_report`, ON DELETE SET NULL, ON UPDATE NO ACTION)
