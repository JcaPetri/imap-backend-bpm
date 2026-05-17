package com.imap.bpm.infrastructure.security;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Snapshot del usuario autenticado (extraído del JWT) durante la request.
 *
 * userId               UUID del usuario (claim "sub" del JWT)
 * email                email del usuario (claim "email")
 * permissionsByTenant  Map<tenantId UUID string, List<permission code>>
 *                      Ej: { "uuid-tenant-1": ["users.read","users.write"], ... }
 *                      Es lo que IAM emite — el system consulta este map para
 *                      autorizar (tenant-aware) sin volver a la DB.
 *
 * impersonation        true si el token es de impersonation (claim opcional)
 *                      Lo usa logging/auditoría para no confundir con login normal.
 *
 * En Iter 6+ podemos extender para incluir roles directos por tenant si IAM
 * los emite.
 */
public record UserContext(
    UUID userId,
    String email,
    Map<String, List<String>> permissionsByTenant,
    boolean impersonation
) {

    /** Convenience: returns true if user has the perm on the given tenant. */
    public boolean hasPermission(UUID tenantId, String permCode) {
        if (tenantId == null || permCode == null || permissionsByTenant == null) return false;
        List<String> perms = permissionsByTenant.get(tenantId.toString());
        return perms != null && perms.contains(permCode);
    }

    /** Convenience: returns true if the user has ANY perm on the tenant (membership). */
    public boolean isMemberOf(UUID tenantId) {
        if (tenantId == null || permissionsByTenant == null) return false;
        List<String> perms = permissionsByTenant.get(tenantId.toString());
        return perms != null && !perms.isEmpty();
    }
}
