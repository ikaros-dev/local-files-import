package run.ikaros.plugin.file;

import org.pf4j.PluginWrapper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import run.ikaros.api.constant.FileConst;
import run.ikaros.api.core.attachment.Attachment;
import run.ikaros.api.core.attachment.AttachmentOperate;
import run.ikaros.api.infra.properties.IkarosProperties;
import run.ikaros.api.infra.utils.FileUtils;
import run.ikaros.api.plugin.BasePlugin;
import run.ikaros.api.store.enums.AttachmentType;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

import static run.ikaros.api.constant.FileConst.DEFAULT_FOLDER_ROOT_ID;
import static run.ikaros.api.core.attachment.AttachmentConst.ROOT_DIRECTORY_ID;

@Slf4j
@Component
public class LocalFilesImportPlugin extends BasePlugin {
    private static final String NAME = "PluginLocalFilesImport";
    private final IkarosProperties ikarosProperties;
    private final AttachmentOperate attachmentOperate;

    public LocalFilesImportPlugin(PluginWrapper wrapper, IkarosProperties ikarosProperties,
                                  AttachmentOperate attachmentOperate) {
        super(wrapper);
        this.ikarosProperties = ikarosProperties;
        this.attachmentOperate = attachmentOperate;
    }


    @Override
    public void start() {
        log.info("plugin [{}] start success", NAME);
        linkFiles();
    }

    @Override
    public void stop() {
        log.info("plugin [{}] stop success", NAME);
    }

    @Override
    public void delete() {
        log.debug("plugin [{}] delete success", NAME);
    }


    @Async
    protected void linkFiles() {
        log.info("start import links dir files.");
        long start = System.currentTimeMillis();
        Path links = ikarosProperties.getWorkDir().resolve(FileConst.DEFAULT_IMPORT_DIR_NAME);
        File linksDir = links.toFile();
        if (linksDir.isFile()) {
            throw new LocalFilesImportException("current links must dir.");
        }
        if (!linksDir.exists()) {
            linksDir.mkdirs();
            log.info("mkdir links dir, path={}", linksDir);
        }

        handleImportDirFile(linksDir, 0L)
            .subscribeOn(Schedulers.parallel())
            .subscribe();

        log.info("end import links dir files, time: {}", System.currentTimeMillis() - start);
    }


    private Mono<Void> handleImportDirFile(File file, Long parentId) {
        if (file.isDirectory()) {
            // ignore . and .. create dir.
            if (".".equalsIgnoreCase(file.getName()) || "..".equalsIgnoreCase(file.getName())) {
                return Mono.empty();
            }
            // dir
            return Mono.just(Attachment.builder()
                    .type(AttachmentType.Directory)
                    .parentId(parentId)
                    .name(file.getName())
                    .updateTime(LocalDateTime.now())
                    .fsPath(file.getAbsolutePath())
                    .build())
                .flatMap(attachmentOperate::save)
                .flatMapMany(attachment -> Flux.fromArray(Objects.requireNonNull(file.listFiles()))
                    .flatMap(file1 -> handleImportDirFile(file1, attachment.getId()))
                )
                .then();
        } else {
            // file
            if (!file.exists()) {
                return Mono.empty();
            }

            // 创建上传文件目录
            String name = file.getName();
            String filePostfix = FileUtils.parseFilePostfix(name);
            String uploadFilePath
                = FileUtils.buildAppUploadFilePath(
                ikarosProperties.getWorkDir().toFile().getAbsolutePath(),
                filePostfix);
            File uploadFile = new File(uploadFilePath);


            try {
                Files.createLink(uploadFile.toPath(), file.toPath());
                log.info("copy file hard link from: {}, to: {}",
                    file.getAbsolutePath(), uploadFile.getAbsolutePath());
            } catch (FileAlreadyExistsException fileAlreadyExistsException) {
                log.warn("file already exists for path: [{}].", uploadFile.getAbsolutePath());
                return null;
            } catch (IOException fileSystemException) {
                // 硬链接失败则进行复制
                log.warn("file hard links fail from: {}, to: {}",
                    file.getAbsolutePath(), uploadFile.getAbsolutePath());
                try {
                    Files.copy(file.toPath(), uploadFile.toPath());
                    log.info("copy file from: {}, to: {}",
                        file.getAbsolutePath(), uploadFile.getAbsolutePath());
                } catch (IOException e) {
                    log.error("skip operate, copy file fail from: {}, to: {}"
                        , file.getAbsolutePath(), uploadFile.getAbsolutePath()
                        , e);
                    throw new RuntimeException(e);
                }
            }

            Attachment attachment = Attachment.builder()
                .parentId(parentId).name(name).type(AttachmentType.File)
                .fsPath(file.getAbsolutePath()).size(file.length()).updateTime(LocalDateTime.now())
                .url(
                    uploadFilePath.replace(ikarosProperties.getWorkDir().toFile().getAbsolutePath(),
                            "")
                        .replace("\\", "/")).build();

            return Mono.just(attachment)
                .flatMap(attachmentOperate::save)
                .then();
        }
    }
}
