import { FastifyError, FastifyReply, FastifyRequest } from 'fastify';
import { ZodError } from 'zod';
import {
  HoldStateConflictError,
  IdempotencyKeyConflictError,
  InsufficientCreditsError,
  PoolConfigurationError,
  RateLimitExceededError,
  ResourceNotFoundError,
  UnsupportedModelError,
} from '../types/credits';
import { OrderCreditHoldCancelledError } from '../services/StreamHoldService';

export function errorHandler(
  error: FastifyError | Error,
  _request: FastifyRequest,
  reply: FastifyReply
) {
  if (error instanceof ZodError) {
    return reply.status(400).send({
      error: 'Validation failed',
      code: 'VALIDATION_ERROR',
      details: error.errors,
    });
  }

  if (error instanceof UnsupportedModelError) {
    return reply.status(400).send({
      error: error.message,
      code: 'UNSUPPORTED_MODEL',
    });
  }

  if (error instanceof ResourceNotFoundError) {
    return reply.status(404).send({
      error: error.message,
      code: 'NOT_FOUND',
    });
  }

  if (error instanceof InsufficientCreditsError) {
    return reply.status(402).send({
      error: error.message,
      code: 'INSUFFICIENT_CREDITS',
    });
  }

  if (error instanceof RateLimitExceededError) {
    reply.header('Retry-After', String(error.retryAfterSeconds));
    return reply.status(429).send({
      error: error.message,
      code: 'CREDIT_BURST_LIMIT_EXCEEDED',
      retryAfterSeconds: error.retryAfterSeconds,
    });
  }

  if (error instanceof PoolConfigurationError) {
    return reply.status(409).send({
      error: error.message,
      code: 'POOL_CONFIGURATION_CONFLICT',
    });
  }

  if (error instanceof OrderCreditHoldCancelledError) {
    return reply.status(409).send({
      error: error.message,
      code: 'HOLD_CANCELLED',
    });
  }

  if (error instanceof HoldStateConflictError) {
    return reply.status(409).send({
      error: error.message,
      code: 'HOLD_STATE_CONFLICT',
    });
  }

  if (error instanceof IdempotencyKeyConflictError) {
    return reply.status(409).send({
      error: error.message,
      code: 'IDEMPOTENCY_KEY_CONFLICT',
    });
  }

  return reply.status(500).send({
    error: error.message || 'Internal Server Error',
    code: 'INTERNAL_ERROR',
  });
}
