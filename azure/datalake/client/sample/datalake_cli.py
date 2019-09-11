import click 
import os
from azure.datalake.client.sample.datalake_tool_manager import DatalakeClientFactory, DatalakeToolManager

@click.group()
@click.option('-a', '--account', required=True, type=str, help="storage account name")
@click.option('--auth_type', type=click.Choice(['environment', 'identity', 'service_principal']), default='identity')
@click.option('--client_id', required=False, type=str)
@click.option('--client_secret', required=False, type=str)
@click.option('--tenant_id', required=False, type=str)
@click.pass_context
def cli(ctx, account, auth_type, client_id, client_secret, tenant_id):
    # Create datalake client for REST Api requests
    #client = DatalakeClientFactory.create_client(credential, account_name)
    if auth_type=="identity":
        client = DatalakeClientFactory.create_client_from_managed_identity(account)
    if auth_type=="environment":
        client = DatalakeClientFactory.create_client_from_environment(account)
    if auth_type=="service_principal":
        client = DatalakeClientFactory.create_client_from_service_principal(account, client_id, client_secret, tenant_id)

    # Create business logic wrapper 
    manager = DatalakeToolManager(client)

    # Initialize context to be reused in inner commands
    ctx.obj = {}
    ctx.obj['manager'] = manager

@cli.command("create_filesystem")
@click.option('-f', '--filesystem', required=True, type=str, help="filesystem name")
@click.pass_context
def create_filesystem_command(ctx, filesystem):
    """This script creates a filesystem in Azure Data Lake Gen2 storage accounts"""
    try:
        ctx.obj['manager'].create_filesystem(filesystem)
    except Exception as e:
        click.echo("Exception happened!")
        #click.echo(click.style(e, fg='green'))
        click.echo(e)

@cli.command("delete_filesystem")
@click.option('-f', '--filesystem', required=True, type=str, help="filesystem name")
@click.pass_context
def delete_filesystem_command(ctx, filesystem):
    """This script deletes a filesystem in Azure Data Lake Gen2 storage accounts"""
    ctx.obj['manager'].delete_filesystem(filesystem)

@cli.command("create_folder")
@click.option('-f', '--filesystem', required=True, type=str, help="filesystem name")
@click.option('-p', '--path', required=True, type=str, help="Folder path to create in filesystem")
@click.pass_context
def create_folder_command(ctx, filesystem, path):
    """This script creates a directory in an Azure Data Lake Gen2 filesystem"""
    ctx.obj['manager'].create_folder(filesystem, path)

@cli.command("update_owner")
@click.option('-f', '--filesystem', required=True, type=str, help="filesystem name")
@click.option('-p', '--path', required=True, type=str, help="Path in datalake filesystem")
@click.option('-o', '--owner', required=True, type=str, help="New owner to set")
@click.pass_context
def update_owner_command(ctx, filesystem, path, owner):
    """This script updates the owner in the selected path"""
    ctx.obj['manager'].update_owner(filesystem, path, owner)

@cli.command("update_group_owner")
@click.option('-f', '--filesystem', required=True, type=str, help="filesystem name")
@click.option('-p', '--path', required=True, type=str, help="Path in datalake filesystem")
@click.option('-o', '--owner', required=True, type=str, help="New owner to set")
@click.pass_context
def update_group_owner_command(ctx, filesystem, path, owner):
    """This script updates the group owner in the selected path"""
    ctx.obj['manager'].update_group_owner(filesystem, path, owner)

@cli.command("get_path_properties")
@click.option('-f', '--filesystem', required=True, type=str, help="filesystem name")
@click.option('-p', '--path', required=True, type=str, help="Path in datalake filesystem")
@click.option('--decode-user-properties/--no-decode-user-properties', default=False)
@click.option('--upn/--no-upn', default=False)
@click.pass_context
def get_path_properties_command(ctx, filesystem, path, decode_user_properties, upn):
    """This script shows properties in the selected path"""
    path_properties = ctx.obj['manager'].get_path_properties(filesystem, path, decode_user_properties=decode_user_properties, upn=upn)
    print(path_properties)

@cli.command("get_path_user_properties")
@click.option('-f', '--filesystem', required=True, type=str, help="filesystem name")
@click.option('-p', '--path', required=True, type=str, help="Path in datalake filesystem")
@click.option('--decode-user-properties/--no-decode-user-properties', default=False)
@click.pass_context
def get_path_user_properties_command(ctx, filesystem, path, decode_user_properties):
    """This script shows properties in the selected path"""
    path_properties = ctx.obj['manager'].get_path_user_properties(filesystem, path, decode_user_properties=decode_user_properties)
    print(path_properties)

@cli.command("get_path_system_properties")
@click.option('-f', '--filesystem', required=True, type=str, help="filesystem name")
@click.option('-p', '--path', required=True, type=str, help="Path in datalake filesystem")
@click.option('--upn/--no-upn', default=False)
@click.pass_context
def get_path_system_properties_command(ctx, filesystem, path, upn):
    """This script shows properties in the selected path"""
    path_properties = ctx.obj['manager'].get_path_system_properties(filesystem, path, upn=upn)
    print(path_properties)

@cli.command("get_path_acl")
@click.option('-f', '--filesystem', required=True, type=str, help="filesystem name")
@click.option('-p', '--path', required=True, type=str, help="Path in datalake filesystem")
@click.pass_context
def get_path_acl_command(ctx, filesystem, path):
    """This script gets existing acl"""
    properties_acl = ctx.obj['manager'].get_path_acl(filesystem, path)
    print(properties_acl)

@cli.command("update_path_acl")
@click.option('-f', '--filesystem', required=True, type=str, help="filesystem name")
@click.option('-p', '--path', required=True, type=str, help="Path in datalake filesystem")
@click.option('--acl', required=True, type=str, help="new acl to set")
@click.pass_context
def update_path_acl_command(ctx, filesystem, path, acl):
    """This script updates existing acl"""
    ctx.obj['manager'].update_path_acl(filesystem, path, acl)

@cli.command("upload_file")
@click.option('-f', '--filesystem', required=True, type=str, help="filesystem name")
@click.option('--source_file', required=True, type=str, help="Source file path in local filesystem")
@click.option('--target_directory', required=True, type=str, help="Target directory in datalake filesystem")
@click.pass_context
def upload_file_command(ctx, filesystem, source_file, target_directory):
    """This script updates existing acl"""
    ctx.obj['manager'].upload_file(filesystem, source_file, target_directory)

@cli.command("list_filesystems")
@click.option('--prefix', required=False, type=str, help="Filters results to filesystems within the specified prefix")
@click.option('--include-acl/--no-include-acl', default=False)
@click.pass_context
def list_filesystems_command(ctx, prefix, include_acl):
    """This script gets list of existing filesystems"""
    items = ctx.obj['manager'].list_filesystems(prefix=prefix, include_acl=include_acl)
    print(items)

@cli.command("list_path_items")
@click.option('-f', '--filesystem', required=True, type=str, help="filesystem name")
@click.option('-p', '--path', required=False, type=str, help="Path for a directory in datalake filesystem")
@click.option('--max-results', required=False, type=int, help="Limits number of results")
@click.option('--recursive/--no-recursive', default=False)
@click.option('--iterate-in-results/--no-iterate-in-results', default=False)
@click.option('--upn/--no-upn', default=False)
@click.pass_context
def list_path_items_command(ctx, filesystem, path, recursive, iterate_in_results, max_results, upn):
    """This script list items in a directory"""
    items = ctx.obj['manager'].list_path_items(
        filesystem, path=path, recursive=recursive, iterate_in_results=iterate_in_results, max_results=max_results, upn=upn)
    print(items)
