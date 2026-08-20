-- Smart Insurance Data Platform - Analytics Queries

-- 1. Customer 360
SELECT
    c.customer_id,
    c.customer_name,
    COUNT(DISTINCT p.policy_id) AS total_policies,
    COALESCE(SUM(DISTINCT p.premium_amount),0) AS total_premium,
    COUNT(DISTINCT cl.claim_id) AS total_claims,
    COALESCE(SUM(DISTINCT cl.claim_amount),0) AS total_claim_amount,
    COALESCE(SUM(DISTINCT pay.payment_amount),0) AS total_payments
FROM customers c
LEFT JOIN policies p ON c.customer_id = p.customer_id
LEFT JOIN claims cl ON p.policy_id = cl.policy_id
LEFT JOIN payments pay ON p.policy_id = pay.policy_id
GROUP BY c.customer_id, c.customer_name;

-- 2. Premium by policy type
SELECT policy_type, COUNT(*) AS policy_count, SUM(premium_amount) AS total_premium
FROM policies
GROUP BY policy_type
ORDER BY total_premium DESC;

-- 3. Claims by status
SELECT claim_status, COUNT(*) AS claim_count, SUM(claim_amount) AS claim_amount
FROM claims
GROUP BY claim_status;

-- 4. Top customers by claim amount
SELECT customer_id, SUM(claim_amount) AS total_claim_amount
FROM claims
GROUP BY customer_id
ORDER BY total_claim_amount DESC
LIMIT 10;

-- 5. Payment status analysis
SELECT payment_status, COUNT(*) AS payment_count, SUM(payment_amount) AS total_amount
FROM payments
GROUP BY payment_status;
