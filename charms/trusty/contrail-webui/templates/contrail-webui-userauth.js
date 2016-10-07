var auth = {};
auth.admin_user = '{{ identity_admin[0]['service_username'] }}';
auth.admin_password = '{{ identity_admin[0]['service_password'] }}';
auth.admin_tenant_name = '{{ identity_admin[0]['service_tenant_name'] }}';

module.exports = auth;

