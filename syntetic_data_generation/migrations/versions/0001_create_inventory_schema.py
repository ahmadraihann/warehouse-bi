from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = '0001_inventory_schema'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # 🔹 1. products
    op.create_table(
        'products',
        sa.Column('product_id', sa.Integer, primary_key=True),
        sa.Column('description', sa.Text),
        sa.Column('size', sa.Text),
        sa.Column('volume', sa.Integer),
        sa.Column('classification', sa.Integer)
    )

    # 🔹 2. vendors
    op.create_table(
        'vendors',
        sa.Column('vendor_id', sa.Integer, primary_key=True),
        sa.Column('vendor_name', sa.Text)
    )

    # 🔹 3. stores
    op.create_table(
        'stores',
        sa.Column('store_id', sa.Integer, primary_key=True),
        sa.Column('city', sa.Text)
    )

    # 🔹 4. purchases (header)
    op.create_table(
        'purchases',
        sa.Column('purchase_id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('vendor_id', sa.Integer, sa.ForeignKey('vendors.vendor_id')),
        sa.Column('po_number', sa.Integer),
        sa.Column('po_date', sa.Date),
        sa.Column('invoice_date', sa.Date),
        sa.Column('pay_date', sa.Date)
    )

    # 🔹 5. purchase_items (detail)
    op.create_table(
        'purchase_items',
        sa.Column('purchase_item_id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('purchase_id', sa.Integer, sa.ForeignKey('purchases.purchase_id')),
        sa.Column('product_id', sa.Integer, sa.ForeignKey('products.product_id')),
        sa.Column('store_id', sa.Integer, sa.ForeignKey('stores.store_id')),
        sa.Column('quantity', sa.Integer),
        sa.Column('purchase_price', sa.Numeric),
        sa.Column('total_amount', sa.Numeric),
        sa.Column('receiving_date', sa.Date)
    )

    # 🔹 6. sales
    op.create_table(
        'sales',
        sa.Column('sales_id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('store_id', sa.Integer, sa.ForeignKey('stores.store_id')),
        sa.Column('product_id', sa.Integer, sa.ForeignKey('products.product_id')),
        sa.Column('sales_date', sa.Date),
        sa.Column('quantity', sa.Integer),
        sa.Column('sales_price', sa.Numeric),
        sa.Column('total_amount', sa.Numeric),
        sa.Column('excise_tax', sa.Numeric)
    )

     # 🔹 7. inventories
    op.create_table(
        'inventories',
        sa.Column('inventory_id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('store_id', sa.Integer, sa.ForeignKey('stores.store_id')),
        sa.Column('product_id', sa.Integer, sa.ForeignKey('products.product_id')),
        sa.Column('quantity', sa.Integer),
        sa.Column('last_updated', sa.Date)
    )

    op.create_unique_constraint('uq_inventory_store_product', 'inventories', ['store_id', 'product_id'])


def downgrade():
    op.drop_table('inventories')
    op.drop_table('sales')
    op.drop_table('purchase_items')
    op.drop_table('purchases')
    op.drop_table('stores')
    op.drop_table('vendors')
    op.drop_table('products')