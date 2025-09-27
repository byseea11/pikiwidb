start_server {tags {"string manifestingest"}} {

    # 测试参数数量错误
    test {manifestingest: wrong number of args} {
        catch {r manifestingest} e
        string match "*wrong number of arguments*" [string tolower $e]
    } {1}

    # 测试正常 ingest
    test {manifestingest: ingest success from prepared manifest} {
        r manifestingest manifest_1758983376565168000_part0.proto
    } {OK}

    # ---------- 数据校验 (从 data_0.json) ----------
    test {manifestingest: verify kv from data_0.json - 1} {
        r get key_012798511019
    } {value_108185132871181479}

    test {manifestingest: verify kv from data_0.json - 2} {
        r get key_043791021279
    } {value_146337713713991103}

    test {manifestingest: verify kv from data_1.json - 1} {
        r get key_078164891058
    } {value_239691110131150146}

    test {manifestingest: verify kv from data_1.json - 2} {
        r get key_100100111634
    } {value_531771181381646742}
}
