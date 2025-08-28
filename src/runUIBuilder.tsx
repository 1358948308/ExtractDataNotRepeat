//@ts-nocheck
import { bitable, UIBuilder, Base, View, Field,FieldType } from "@lark-base-open/js-sdk";

export default async function main(uiBuilder: UIBuilder, { t }) {
    uiBuilder.form((form) => ({
        formItems: [
            form.tableSelect('table', { label: '选择源表' }),
            form.fieldSelect('field', { label: '选择字段',filterByTypes:[FieldType.Text], sourceTable: 'table', multiple: true }),
            form.tableSelect('totable', { label: '选择插入表' }),
            form.fieldSelect('tofield', { label: '选择字段',filterByTypes:[FieldType.Text], sourceTable: 'totable', multiple: true }),
        ],
        buttons: ['确定'],
    }), async ({ key, values }) => {
        const { table, field, totable, tofield } = values;
        uiBuilder.showLoading('0%');
        const tableItem = await bitable.base.getTableById(table.id);
        const recordIdList = await table.getRecordIdList();
        const recordlen = recordIdList.length;
        
        // 预获取所有字段信息，避免重复请求
        const fieldItems = await Promise.all(field.map(item => tableItem.getFieldById(item.id)));
        const fieldNames = await Promise.all(fieldItems.map(field => field.getName()));
        
        // 批量处理记录，每批处理50条
        const batchSize = 50;
        const totalBatches = Math.ceil(recordlen / batchSize);
        let recordListMap = new Map();
        
        for (let batch = 0; batch < totalBatches; batch++) {
            const startIndex = batch * batchSize;
            const endIndex = Math.min(startIndex + batchSize, recordlen);
            const batchRecordIds = recordIdList.slice(startIndex, endIndex);
            
            // 并行处理一批记录
            const batchRecords = await Promise.all(batchRecordIds.map(async (recordId) => {
                let record = new Map();
                let recordname = '';
                
                // 并行获取字段值
                await Promise.all(fieldItems.map(async (fieldItem, index) => {
                    const fieldval = await fieldItem.getValue(recordId);
                    const fieldname = fieldNames[index];
                    record.set(fieldname, fieldval?fieldval[0].text:"");
                    recordname += fieldval?fieldval[0].text:"";
                }));
                
                return { recordname, record };
            }));
            
            // 添加到去重Map
            batchRecords.forEach(({ recordname, record }) => {
                recordListMap.set(recordname, record);
            });
            
            // 更新进度
            const processed = Math.min((batch + 1) * batchSize, recordlen);
            uiBuilder.showLoading(`${Math.floor(100 * processed / recordlen)}%    ${processed}/${recordlen}`);
        }

        // 获取目标表
        const toTableItem = await bitable.base.getTableById(totable.id);
        
        // 准备批量插入的记录数据
        const newRecords = [];
        recordListMap.forEach((recordMap) => {
            const newRecord = {};
            
            // 更高效的字段映射
            field.forEach((sourceField, index) => {
                if (tofield && tofield[index]) {
                    const sourceFieldName = fieldNames[index];
                    const targetFieldId = tofield[index].id;
                    newRecord[targetFieldId] = recordMap.get(sourceFieldName);
                }
            });
            
            newRecords.push({'fields':newRecord});
        });
        
        uiBuilder.showLoading(`去重完成，共 ${newRecords.length} 条记录待插入`);
        // 批量插入记录 - 分批次处理以避免请求过大
        if (newRecords.length > 0) {
            // 动态调整批次大小，根据记录数量
            const batchSize = Math.min(500, Math.max(100, Math.floor(newRecords.length / 10)));
            const totalBatches = Math.ceil(newRecords.length / batchSize);
            let successCount = 0;
            
            for (let batch = 0; batch < totalBatches; batch++) {
                const startIndex = batch * batchSize;
                const endIndex = Math.min(startIndex + batchSize, newRecords.length);
                const batchRecords = newRecords.slice(startIndex, endIndex);
                
                try {
                    const progress = startIndex + batchRecords.length;
                    uiBuilder.showLoading(`插入中 ${progress}/${newRecords.length}`);
                    await toTableItem.addRecords(batchRecords);
                    successCount += batchRecords.length;
                } catch (error) {
                    console.error(`第 ${batch + 1} 批次插入失败:`, error);
                    // 可以选择重试失败的批次
                }
            }
            
            uiBuilder.message.success(`成功插入 ${successCount}/${newRecords.length} 条记录`);
        } else {
            uiBuilder.message.info('没有找到需要插入的新记录');
        }
        
        uiBuilder.hideLoading();
    });
}