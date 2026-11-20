import { withTransaction } from '../db';
import { balanceCacheKey, redis } from '../redis';
import { PoolConfigurationError, ResourceNotFoundError } from '../types/credits';
import { clientQuery, loadOrgChain, lockFullWallet, MAX_POOL_LEVELS } from './OrgWallet';

/**
 * Hierarchical credit pooling configuration: attach a sub-org to a parent whose shared pool absorbs
 * the sub-org's deduction and reservation deficits.
 */
export class OrgPoolService {
  /**
   * Set (or clear) an org's parent. Rejected when it would create a cycle, exceed MAX_POOL_LEVELS,
   * or while holds in the org's subtree have portions cascaded into other orgs (those portions
   * must stay within each consumer's chain so they can be locked in canonical order).
   *
   * Takes a FULL lock on the org: any cascade passing through it locks the org too, so re-parenting
   * serializes with in-flight cascades.
   */
  async setParent(
    orgId: string,
    parentOrgId: string | null
  ): Promise<{ orgId: string; parentOrgId: string | null; chain: string[] }> {
    const result = await withTransaction(async (client) => {
      const run = clientQuery(client);
      const [org] = await loadOrgChain(run, orgId);
      if (parentOrgId === orgId) {
        throw new PoolConfigurationError(`Org ${orgId} cannot be its own parent`);
      }

      let parentChain: string[] = [];
      if (parentOrgId) {
        parentChain = (await loadOrgChain(run, parentOrgId).catch((err) => {
          if (err instanceof ResourceNotFoundError) throw new ResourceNotFoundError('Organization', parentOrgId);
          throw err;
        })).map((o) => o.orgId);
        if (parentChain.includes(orgId)) {
          throw new PoolConfigurationError(`Setting ${parentOrgId} as parent of ${orgId} would create a cycle`);
        }
      }

      // Height of the subtree below `orgId` (1 = no children).
      const heightRes = await client.query<{ height: number }>(
        `WITH RECURSIVE subtree AS (
           SELECT org_id, 1 AS level FROM org_balances WHERE org_id = $1
           UNION ALL
           SELECT c.org_id, s.level + 1 FROM org_balances c JOIN subtree s ON c.parent_org_id = s.org_id
           WHERE s.level <= $2
         )
         SELECT MAX(level) AS height FROM subtree`,
        [orgId, MAX_POOL_LEVELS + 1]
      );
      if (Number(heightRes.rows[0].height) + parentChain.length > MAX_POOL_LEVELS) {
        throw new PoolConfigurationError(`Org hierarchies are limited to ${MAX_POOL_LEVELS} levels`);
      }

      await lockFullWallet(client, orgId, org);

      const cascaded = await client.query(
        `WITH RECURSIVE subtree AS (
           SELECT org_id FROM org_balances WHERE org_id = $1
           UNION ALL
           SELECT c.org_id FROM org_balances c JOIN subtree s ON c.parent_org_id = s.org_id
         )
         SELECT 1
         FROM credit_holds h
         JOIN credit_hold_portions p ON p.hold_id = h.hold_id
         WHERE h.status = 'ACTIVE' AND p.org_id <> h.org_id AND h.org_id IN (SELECT org_id FROM subtree)
         LIMIT 1`,
        [orgId]
      );
      if (cascaded.rowCount) {
        throw new PoolConfigurationError(
          `Org ${orgId} (or a sub-org) has active holds reserved in a parent pool; settle or release them first`
        );
      }

      await client.query(`UPDATE org_balances SET parent_org_id = $1, updated_at = NOW() WHERE org_id = $2`, [
        parentOrgId,
        orgId,
      ]);
      return { orgId, parentOrgId, chain: [orgId, ...parentChain] };
    });
    await redis.del(balanceCacheKey(orgId));
    return result;
  }
}

export const orgPoolService = new OrgPoolService();
