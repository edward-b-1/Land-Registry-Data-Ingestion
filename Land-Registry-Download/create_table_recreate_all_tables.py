
import create_table_pp_monthly_update_download_file_log
import create_table_pp_monthly_update_archive_file_log
import create_table_pp_complete_download_file_log
import create_table_pp_complete_archive_file_log
import create_table_pp_complete_data
import create_table_pp_monthly_data
import create_table_pp_monthly_data_operation_log


def main():
    create_table_pp_monthly_update_download_file_log.main(recreate=True)
    create_table_pp_monthly_update_archive_file_log.main(recreate=True)
    create_table_pp_complete_download_file_log.main(recreate=True)
    create_table_pp_complete_archive_file_log.main(recreate=True)
    create_table_pp_complete_data.main(recreate=True)
    create_table_pp_monthly_data.main(recreate=True)


if __name__ == '__main__':
    main()
