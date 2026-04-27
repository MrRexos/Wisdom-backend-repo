"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const Stripe = require("stripe");

const STRIPE_API_VERSION = "2026-02-25.clover";
const runStripeE2E = process.env.STRIPE_E2E === "1";
const stripeSecretKey = String(process.env.STRIPE_SECRET_KEY || "").trim();
const e2eCurrency = String(process.env.STRIPE_E2E_CURRENCY || "eur").trim().toLowerCase();
const webhookBaseUrl = String(process.env.WISDOM_E2E_BASE_URL || "").trim().replace(/\/+$/, "");
const webhookSecret = String(process.env.WISDOM_E2E_WEBHOOK_SECRET || process.env.STRIPE_WEBHOOK_SECRET || "").trim();
const connectedAccountId = String(process.env.STRIPE_E2E_CONNECTED_ACCOUNT_ID || "").trim();

function stripeE2ESkipReason(extraRequirement = true) {
  if (!runStripeE2E) return "Set STRIPE_E2E=1 to run Stripe test-mode E2E checks.";
  if (!stripeSecretKey.startsWith("sk_test_")) return "STRIPE_SECRET_KEY must be a Stripe test-mode key.";
  if (!extraRequirement) return "Missing optional environment required for this E2E check.";
  return false;
}

function getStripeClient() {
  const skipReason = stripeE2ESkipReason();
  if (skipReason) {
    throw new Error(skipReason);
  }

  return new Stripe(stripeSecretKey, {
    apiVersion: STRIPE_API_VERSION,
  });
}

async function deleteCustomerQuietly(stripe, customerId) {
  if (!customerId) return;
  try {
    await stripe.customers.del(customerId);
  } catch {}
}

function buildSignedWebhookPayload(event, secret) {
  const payload = JSON.stringify(event);
  const timestamp = Math.floor(Date.now() / 1000);
  const signature = crypto
    .createHmac("sha256", secret)
    .update(`${timestamp}.${payload}`, "utf8")
    .digest("hex");

  return {
    payload,
    signatureHeader: `t=${timestamp},v1=${signature}`,
  };
}

test("Stripe test mode: deposit succeeds and can be refunded", {
  skip: stripeE2ESkipReason(),
}, async () => {
  const stripe = getStripeClient();
  let customerId = null;
  let paymentIntentId = null;

  try {
    const customer = await stripe.customers.create({
      email: "stripe-e2e-deposit@wisdom.test",
      metadata: { source: "wisdom_stripe_booking_flow_e2e" },
    });
    customerId = customer.id;

    const intent = await stripe.paymentIntents.create({
      amount: 100,
      currency: e2eCurrency,
      customer: customerId,
      payment_method: "pm_card_visa",
      confirm: true,
      automatic_payment_methods: { enabled: true, allow_redirects: "never" },
      metadata: {
        booking_id: "e2e-deposit",
        type: "deposit",
      },
    });
    paymentIntentId = intent.id;

    assert.equal(intent.status, "succeeded");
    assert.equal(intent.customer, customerId);
    assert.equal(intent.amount_received, 100);

    const refund = await stripe.refunds.create({
      payment_intent: paymentIntentId,
      amount: 100,
      metadata: { source: "wisdom_stripe_booking_flow_e2e" },
    });

    assert.equal(refund.status, "succeeded");
    assert.equal(refund.amount, 100);
  } finally {
    await deleteCustomerQuietly(stripe, customerId);
  }
});

test("Stripe test mode: SCA card requires 3D Secure action", {
  skip: stripeE2ESkipReason(),
}, async () => {
  const stripe = getStripeClient();
  let customerId = null;

  try {
    const customer = await stripe.customers.create({
      email: "stripe-e2e-sca@wisdom.test",
      metadata: { source: "wisdom_stripe_booking_flow_e2e" },
    });
    customerId = customer.id;

    const intent = await stripe.paymentIntents.create({
      amount: 100,
      currency: e2eCurrency,
      customer: customerId,
      payment_method: "pm_card_authenticationRequired",
      confirm: true,
      return_url: "wisdomexpo://stripe-return",
      automatic_payment_methods: { enabled: true, allow_redirects: "never" },
      metadata: {
        booking_id: "e2e-sca",
        type: "deposit",
      },
    });

    assert.equal(intent.status, "requires_action");
    assert.ok(intent.next_action);
    assert.equal(intent.customer, customerId);
  } finally {
    await deleteCustomerQuietly(stripe, customerId);
  }
});

test("Stripe webhook endpoint ignores duplicate event deliveries", {
  skip: stripeE2ESkipReason(Boolean(webhookBaseUrl && webhookSecret)),
}, async () => {
  const eventId = `evt_wisdom_e2e_${Date.now()}`;
  const event = {
    id: eventId,
    object: "event",
    api_version: STRIPE_API_VERSION,
    created: Math.floor(Date.now() / 1000),
    livemode: false,
    type: "payment_intent.processing",
    data: {
      object: {
        id: `pi_wisdom_e2e_${Date.now()}`,
        object: "payment_intent",
        amount: 100,
        amount_received: 0,
        currency: e2eCurrency,
        customer: null,
        metadata: {},
        status: "processing",
      },
    },
  };
  const { payload, signatureHeader } = buildSignedWebhookPayload(event, webhookSecret);
  const url = `${webhookBaseUrl}/webhooks/stripe`;
  const requestOptions = {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "stripe-signature": signatureHeader,
    },
    body: payload,
  };

  const firstResponse = await fetch(url, requestOptions);
  const firstBody = await firstResponse.json();
  assert.equal(firstResponse.status, 200);
  assert.equal(firstBody.received, true);

  const secondResponse = await fetch(url, requestOptions);
  const secondBody = await secondResponse.json();
  assert.equal(secondResponse.status, 200);
  assert.equal(secondBody.received, true);
  assert.equal(secondBody.duplicate, true);
});

test("Stripe Connect test mode: provider transfer can be created idempotently", {
  skip: stripeE2ESkipReason(connectedAccountId.startsWith("acct_")),
}, async () => {
  const stripe = getStripeClient();
  const transferGroup = `booking-e2e-${Date.now()}`;
  const idempotencyKey = `wisdom-e2e-transfer-${Date.now()}`;

  const transfer = await stripe.transfers.create(
    {
      amount: 50,
      currency: e2eCurrency,
      destination: connectedAccountId,
      transfer_group: transferGroup,
      metadata: {
        booking_id: "e2e-transfer",
        booking_purpose: "provider_payout",
        source: "wisdom_stripe_booking_flow_e2e",
      },
    },
    { idempotencyKey }
  );

  assert.equal(transfer.amount, 50);
  assert.equal(transfer.currency, e2eCurrency);
  assert.equal(transfer.destination, connectedAccountId);
  assert.equal(transfer.transfer_group, transferGroup);
});
