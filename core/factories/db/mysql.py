def build_mysql_procedure(procedure, args):
    "MySql Stored procedure configuration."
    if isinstance(args, str):
        return f"CALL {procedure} ('{args}')"
    elif len(args) == 1 and isinstance(args, list):
        return f"CALL {procedure} ('{args[0]}')"
    elif isinstance(args, dict):
        values = list(args.values())
    else:
        values = args
        
    # Create formatting string for args    
    arg_values = f"""('{"', '".join(map(str, values))}')"""
    
    # Fixes for None values and empty strings for stored procedures that
    # have optional values
    arg_values = arg_values.replace("'None'", 'NULL').replace("''''", "''")

    # Return formatted query string
    return f"CALL {procedure}{arg_values}"
